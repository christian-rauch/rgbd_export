#!/usr/bin/env python3
import rosbag
from cv_bridge import CvBridge
from sensor_msgs.msg import Image, CompressedImage
from geometry_msgs.msg import TransformStamped
import tf2_ros
import genpy

import os
import numpy as np
import cv2
import struct
import json

import argparse


class RGBDExporter:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("bag_file", type=str, help="path to rosbag file")
        parser.add_argument("export_dir", type=str, help="path to export folder")
        parser.add_argument("--topic_rgb", type=str, default="/camera/rgb/image_rect_color/compressed", help="colour topic (CompressedImage)")
        parser.add_argument("--topic_depth", type=str, default="/camera/depth/image_rect_raw/compressedDepth", help="depth topic (CompressedImage)")
        parser.add_argument("--topic_camera_info", type=str, default="/camera/rgb/camera_info", help="camera info topic (CameraInfo)")
        parser.add_argument("--topic_joints", type=str, default="/joint_states", help="joint state topic (JointState)")
        parser.add_argument("-f", "--force", action="store_true", help="overwrite old data")
        parser.add_argument("-b", "--reference_frame", type=str, default="base", help="parent frame of camera pose")

        args = parser.parse_args()

        # input/output paths
        bag_file_path = args.bag_file
        self.export_path = args.export_dir

        if os.path.exists(self.export_path) and not args.force:
            raise UserWarning("path "+self.export_path+" already exists!")

        # image topics with CompressedImage
        self.topic_rgb = args.topic_rgb
        self.topic_depth = args.topic_depth
        # camera intrinsics with CameraInfo
        self.topic_ci = args.topic_camera_info
        # topic with JointState
        self.topic_joints = args.topic_joints

        self.topics_tf = ["/tf", "/tf_static"]

        self.topics = [self.topic_rgb, self.topic_depth, self.topic_ci, self.topic_joints]

        self.ref_frame = args.reference_frame

        bag_file_path = os.path.expanduser(bag_file_path)
        print("reading:", bag_file_path)
        self.bag = rosbag.Bag(bag_file_path, mode='r')
        print("duration:", self.bag.get_end_time()-self.bag.get_start_time(),"s")

        self.export_path = os.path.expanduser(self.export_path)
        self.path_colour = os.path.join(self.export_path, "colour")
        self.path_depth = os.path.join(self.export_path, "depth")

        self.cvbridge = CvBridge()

    def export(self):
        print("exporting to: "+self.export_path)

        # create export directories
        for dir_path in [self.path_colour, self.path_depth]:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

        ref_times = []

        # fill the tf buffer
        # maximum duration to cache all transformations
        # https://github.com/ros/geometry2/issues/356
        time_max = genpy.Duration(secs=pow(2, 31) - 1)
        tf_buffer = tf2_ros.Buffer(cache_time=time_max, debug=False)
        for topic, msg, t in self.bag.read_messages(topics=self.topics_tf):
            for transf in msg.transforms:
                # convert to geometry_msgs/TransformStamped
                bag_msg = TransformStamped()
                bag_msg.header = transf.header
                bag_msg.child_frame_id = transf.child_frame_id
                bag_msg.transform.translation.x = transf.transform.translation.x
                bag_msg.transform.translation.y = transf.transform.translation.y
                bag_msg.transform.translation.z = transf.transform.translation.z
                bag_msg.transform.rotation.w = transf.transform.rotation.w
                bag_msg.transform.rotation.x = transf.transform.rotation.x
                bag_msg.transform.rotation.y = transf.transform.rotation.y
                bag_msg.transform.rotation.z = transf.transform.rotation.z
                tf_buffer.set_transform(bag_msg, "exporter")

        # get timestamps for all messages to select reference topic with smallest amount of messages
        # get available joint names and their oldest value, e.g. we are looking into the future and assume
        # that the first seen joint value reflects the state of the joint before this time
        topic_times = dict()
        for t in self.topics:
            topic_times[t] = []
        full_jnt_values = dict() # store first (oldest) joint values of complete set
        for topic, msg, t in self.bag.read_messages(topics=self.topics):
            # get set of joints
            if topic == self.topic_joints:
                for ijoint in range(len(msg.name)):
                    if msg.name[ijoint] not in full_jnt_values:
                        full_jnt_values[msg.name[ijoint]] = msg.position[ijoint]
            topic_times[topic].append(msg.header.stamp)

        if len(topic_times[self.topic_joints])==0:
            print("Ignoring joint topic.")
        else:
            print("joints:", full_jnt_values.keys())

        if len(topic_times[self.topic_rgb])==0:
            print("NO colour images. Check that topic '"+self.topic_rgb+"' is present in bag file!")
        if len(topic_times[self.topic_depth]) == 0:
            print("NO depth images. Check that topic '"+self.topic_depth+"' is present in bag file!")

        # remove topics with no messages
        [topic_times.pop(top, None) for top in list(topic_times.keys()) if len(topic_times[top]) == 0]

        if not topic_times:
            bag_topics = self.bag.get_type_and_topic_info().topics
            print("Found no messages on any of the given topics.")
            print("Valid topics are:", bag_topics.keys())
            print("Given topics are:", self.topics)

        full_joint_list_sorted = sorted(full_jnt_values.keys())
        joint_states = []
        camera_poses = []

        ref_topic = self.topic_rgb

        # sample and hold synchronisation
        sync_msg = dict()
        for top in topic_times.keys():
            sync_msg[top] = None
        has_all_msg = False

        camera_info = None
        for topic, msg, t in self.bag.read_messages(topics=[self.topic_ci]):
            # export a single camera info message
            camera_info = msg
            cp = {'cx': msg.K[2], 'cy': msg.K[5], 'fu': msg.K[0], 'fv': msg.K[4],
                  'width': msg.width, 'height': msg.height}
            with open(os.path.join(self.export_path, "camera_parameters.json"), 'w') as f:
                json.dump(cp, f, indent=4, separators=(',', ': '), sort_keys=True)
            break

        if camera_info is None:
            raise Exception("No CameraInfo message received!")

        for topic, msg, t in self.bag.read_messages(topics=topic_times.keys()):
            if topic == self.topic_joints:
                # merge all received joints
                for ijoint in range(len(msg.name)):
                    full_jnt_values[msg.name[ijoint]] = msg.position[ijoint]
                    sync_msg[topic] = full_jnt_values
            else:
                # keep the newest message
                sync_msg[topic] = msg

            # export at occurrence of reference message and if all remaining messages have been received
            # e.g. we export the reference message and the newest messages of other topics (sample and hold)
            if topic==ref_topic and (has_all_msg or all([v is not None for v in sync_msg.values()])):
                # faster evaluation of previous statement
                has_all_msg = True

                # export
                ref_time = msg.header.stamp
                ref_times.append(ref_time.to_nsec())

                # get transformations
                try:
                    camera_pose = tf_buffer.lookup_transform(
                        target_frame=self.ref_frame, source_frame=camera_info.header.frame_id,
                        time=ref_time)
                    p = camera_pose.transform.translation
                    q = camera_pose.transform.rotation
                    camera_poses.append([p.x, p.y, p.z, q.w, q.x, q.y, q.z])
                except (tf2_ros.LookupException, tf2_ros.ConnectivityException, tf2_ros.ExtrapolationException):
                    pass

                for sync_topic in sync_msg.keys():
                    if sync_topic == self.topic_joints:
                        # export full joints, sorted by joint name
                        jvalues = []
                        for jname in full_joint_list_sorted:
                            jvalues.append(sync_msg[sync_topic][jname])
                        joint_states.append(jvalues)

                    elif sync_topic == self.topic_rgb:
                        # export RGB
                        if sync_msg[sync_topic]._type == Image._type:
                            if sync_msg[sync_topic].encoding[:6] == "bayer_":
                                desired_encoding = "bgr8"
                            else:
                                desired_encoding = "passthrough"
                            colour_img = self.cvbridge.imgmsg_to_cv2(sync_msg[sync_topic], desired_encoding=desired_encoding)
                        elif sync_msg[sync_topic]._type == CompressedImage._type:
                            colour_img = self.cvbridge.compressed_imgmsg_to_cv2(sync_msg[sync_topic])
                        else:
                            print("unsupported:",sync_msg[sync_topic]._type)
                        cv2.imwrite(os.path.join(self.path_colour, "colour_" + str(ref_time) + ".png"), colour_img)

                    elif sync_topic == self.topic_depth and sync_msg[sync_topic]._type == CompressedImage._type:
                        # export depth
                        depth_fmt, compr_type = sync_msg[sync_topic].format.split(';')
                        # remove white space
                        depth_fmt = depth_fmt.strip()
                        compr_type = compr_type.strip()

                        if compr_type == "compressedDepth":
                            # remove header from raw data
                            # C header definition at:
                            # /opt/ros/indigo/include/compressed_depth_image_transport/compression_common.h
                            # enum compressionFormat {
                            #   UNDEFINED = -1, INV_DEPTH
                            # };
                            # struct ConfigHeader {
                            #   compressionFormat format;
                            #   float depthParam[2];
                            # };
                            # header size = enum (4 byte) + float[2] (2 x 4 byte) = 12 byte
                            # enum size may vary and needs to be adapted if decoding fails
                            depth_header_size = 12
                            raw_data = sync_msg[sync_topic].data[depth_header_size:]

                            depth_img_raw = cv2.imdecode(np.frombuffer(raw_data, np.uint8), cv2.IMREAD_UNCHANGED)
                            if depth_img_raw is None:
                                # probably wrong header size
                                raise Exception("Could not decode compressed depth image."
                                                "You may need to change 'depth_header_size'!")

                            if depth_fmt == "16UC1":
                                # write raw image data
                                depth_img = depth_img_raw
                            elif depth_fmt == "32FC1":
                                raw_header = sync_msg[sync_topic].data[:depth_header_size]
                                # header: int, float, float
                                [compfmt, depthQuantA, depthQuantB] = struct.unpack('iff', raw_header)
                                depth_img_scaled = depthQuantA / (depth_img_raw.astype(np.float32) - depthQuantB)
                                # filter max values
                                depth_img_scaled[depth_img_raw == 0] = 0

                                # depth_img_scaled provides distance in meters as f32
                                # for storing it as png, we need to convert it to 16UC1 again (depth in mm)
                                depth_img_mm = (depth_img_scaled * 1000).astype(np.uint16)
                                depth_img = depth_img_mm
                            else:
                                raise Exception("Decoding of '"+sync_msg[sync_topic].format+"' is not implemented!")

                        else:
                            if depth_fmt == "16UC1":
                                # assume that all 16bit image representations can be decoded by opencv
                                rawimgdata = sync_msg[sync_topic].data
                                depth_img = cv2.imdecode(np.frombuffer(rawimgdata, np.uint8), cv2.IMREAD_UNCHANGED)
                            else:
                                raise Exception("Decoding of '" + sync_msg[sync_topic].format + "' is not implemented!")

                        # write image
                        cv2.imwrite(os.path.join(self.path_depth, "depth_" + str(ref_time) + ".png"), depth_img)

                    elif sync_topic == self.topic_depth and sync_msg[sync_topic]._type == Image._type:
                        depth_img = self.cvbridge.imgmsg_to_cv2(sync_msg[sync_topic])
                        cv2.imwrite(os.path.join(self.path_depth, "depth_" + str(ref_time) + ".png"), depth_img)

        np.savetxt(os.path.join(self.export_path, "time.csv"), ref_times, fmt="%i")

        if len(camera_poses)>0:
            np.savetxt(os.path.join(self.export_path, "camera_pose.csv"), camera_poses,
                       fmt="%.8f", header="px py pz qw qx qy qz", delimiter=" ", comments="")
        else:
            print("No camera poses have been extracted. Make sure that the given reference frame '" + self.ref_frame + "' exists.")

        if len(joint_states)>0:
            np.savetxt(os.path.join(self.export_path, "joints.csv"), joint_states,
                       fmt="%.8f", header=(" ").join(full_joint_list_sorted), delimiter=" ", comments="")

        print("done")

    def __del__(self):
        # close log file
        if hasattr(self, 'bag'):
            self.bag.close()


if __name__ == '__main__':
    RGBDExporter().export()
