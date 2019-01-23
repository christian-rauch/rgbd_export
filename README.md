# rosbag exporter for compressed images and joint states

Synchronised export of compressed colour (topic compressed) and depth (topic compressedDepth) images, and joint states.

```
rosrun rgbd_export rgbd_sync_export.py \
  ~/rosbags/experiment1.bag \
  ~/rgb_log_exp/exp1 \
  --topic_rgb /camera/rgb/image_rect_color/compressed \
  --topic_depth /camera/depth/image_rect_raw/compressedDepth \
  --topic_camera_info /camera/rgb/camera_info \
  --topic_joints /joint_states
```
