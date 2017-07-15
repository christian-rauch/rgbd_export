#!/usr/bin/env python

from __future__ import print_function
import rosbag
import rospy
from sensor_msgs.msg import JointState, CompressedImage

import os
import numpy as np
import cv2


class RGBDExporter:
    def __init__(self, node_name):
        rospy.init_node(node_name)

        # read parameters
        try:
            # input/output paths
            bag_file_path = rospy.get_param('~bag_file')
            self.export_path = rospy.get_param('~export_dir')
            # image topics with CompressedImage
            self.topic_rgb = rospy.get_param('~topic_rgb', default="/camera/rgb/image_rect_color/compressed")
            self.topic_depth = rospy.get_param('~topic_depth', default="/camera/depth/image_rect_raw/compressedDepth")
            # topic with JointState
            self.topic_joints = rospy.get_param('~topic_joints', default="/joint_states")
        except KeyError as e:
            print(e.message+" is undefined")
            return

        print(bag_file_path)
        self.bag = rosbag.Bag(bag_file_path, mode='r')

    def export(self):
        self.export_path = os.path.expanduser(self.export_path)
        print("exporting to: "+self.export_path)
        # create export directories
        path_colour = os.path.join(self.export_path, "colour")
        path_depth = os.path.join(self.export_path, "depth")
        for dir_path in [path_colour, path_depth]:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

        # get timestamps for all messages for synchronisation
        time_rgb = []
        time_depth = []
        time_joints = []
        for topic, msg, t in self.bag.read_messages(topics=[self.topic_rgb, self.topic_depth, self.topic_joints]):
            if topic == self.topic_rgb:
                time_rgb.append(msg.header.stamp)
            elif topic == self.topic_depth:
                time_depth.append(msg.header.stamp)
            elif topic == self.topic_joints:
                time_joints.append(msg.header.stamp)
            else:
                raise Exception("unknown topic")

        # sort timestamps
        time_rgb.sort()
        time_depth.sort()
        time_joints.sort()

        # export without sync
        for topic, msg, t in self.bag.read_messages(topics=[self.topic_rgb, self.topic_depth]):
            if topic==self.topic_rgb:
                # http://wiki.ros.org/rospy_tutorials/Tutorials/WritingImagePublisherSubscriber
                colour_img = cv2.imdecode(np.fromstring(msg.data, np.uint8), cv2.CV_LOAD_IMAGE_COLOR)
                cv2.imwrite(os.path.join(path_colour, "colour_"+str(msg.header.stamp)+".png"), colour_img)
            elif topic == self.topic_depth:
                depth_fmt, compr_type = msg.format.split(';')
                # remove white space
                depth_fmt = depth_fmt.strip()
                compr_type = compr_type.strip()
                if compr_type != "compressedDepth":
                    raise Exception("Compression type is not 'compressedDepth'."
                                    "You probably subscribed to the wrong topic.")

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
                raw_data = msg.data[depth_header_size:]

                if depth_fmt == "16UC1":
                    depth_img = cv2.imdecode(np.fromstring(raw_data, np.uint8), cv2.CV_LOAD_IMAGE_UNCHANGED)
                    if depth_img is not None:
                        cv2.imwrite(os.path.join(path_depth, "depth_" + str(msg.header.stamp) + ".png"), depth_img)
                    else:
                        # probably wrong header size
                        raise Exception("Could not decode compressed depth image."
                                        "You may need to change 'depth_header_size'!")
                else:
                    # check at: image_transport_plugins/compressed_depth_image_transport/src/codec.cpp
                    # on how to decode 32bit float images, you will need the depth quantisation
                    # values (depthQuantA, depthQuantB) from the header for this
                    # e.g.:
                    # import struct
                    # raw_header = msg.data[:depth_header_size]
                    # header: int, float, float
                    # [compfmt, depthQuantA, depthQuantB] = struct.unpack('iff', raw_header)
                    raise Exception("I don't know how to decode '"+depth_fmt+"'")


    def __del__(self):
        # delete all private parameters
        rospy.delete_param(rospy.get_name())
        # close log file
        try:
            self.bag.close()
        except AttributeError:
            pass


if __name__ == '__main__':
    exporter = RGBDExporter("rgbd_exporter")
    exporter.export()
