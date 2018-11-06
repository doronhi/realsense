import rospy
from dynamic_reconfigure.server import Server
from ddynamic_reconfigure_python.ddynamic_reconfigure import DDynamicReconfigure
import pyrealsense2 as rs
import sensor_msgs.msg
from cv_bridge import CvBridge, CvBridgeError
import numpy as np
import constants
import json


def say(name):
    rospy.loginfo('Hello ' + name)

type_to_name = {rs.stream.depth: 'depth',
                rs.stream.infrared: 'infra',
                rs.stream.color: 'color'}

format_to_cvtype = {rs.format.z16: 'mono16',
                    rs.format.y8: 'mono8',
                    rs.format.rgb8: 'rgb8'
                    }

stream_to_format = {rs.stream.depth: rs.format.z16,
                    rs.stream.infrared: rs.format.y8,
                    rs.stream.color: rs.format.rgb8,
                    rs.stream.fisheye: rs.format.raw8,
                    rs.stream.gyro: rs.format.motion_xyz32f,
                    rs.stream.accel: rs.format.motion_xyz32f,
                    }

class rs2_wrapper:
    def __init__(self):
        self.pipeline = self.get_pipeline()
        self.set_initial_params()
        self.start_filters()
        self.publish_options()

        self.publishers = dict()
        self.bridge = CvBridge()
        self.seq = dict()   # Will contain sequential number for each topic key (stream, index, format)
        self.camera_info = dict()

    def create_callback(self, sensor, name):
        def callback(config, level):
            for param_name, value in config.items():
                if param_name not in rs.option.__members__:
                    continue
                option = rs.option.__members__[param_name]
                if sensor.get_option(option) != value:
                    rospy.loginfo('update %s:%s to %s' % (name, param_name, value))
                    sensor.set_option(option ,value)
            return config
        return callback

    def start_filters(self):
        self.filters = []
        # import pdb; pdb.set_trace()
        # filters_info = json.loads(rospy.get_param('~filters_info', '{}'));
        filters_str = rospy.get_param('~filters', '')
        use_disparity_filter = False
        use_colorizer_filter = False
        for filter_str in filters_str.split(','):
            if filter_str == 'disparity':
                use_disparity_filter = True
            elif filter_str == 'colorizer':
                use_colorizer_filter = True
            else:
                self.add_filter(filter_str)

        if use_disparity_filter:
            rospy.loginfo('Add Filter: disparity')
            self.filters = [{'name': 'disparity_start', 'filter': rs.disparity_transform()}] + self.filters
            self.filters.append({'name': 'disparity_end', 'filter': rs.disparity_transform(False)})
            rospy.loginfo('Done add Filter: disparity')
        if use_colorizer_filter:
            self.add_filter('colorizer')

    def add_filter(self, filter_str):
        filter_types = {'colorizer': rs.colorizer(),
                        'temporal': rs.temporal_filter()}
        if filter_str in filter_types:
            self.filters.append({'name': filter_str, 'filter': filter_types[filter_str]})



    def publish_options(self):
        self.ddynrec = dict()
        for sensor in self.pipeline.get_active_profile().get_device().sensors:
            name = sensor.get_info(rs.camera_info.name)
            self.publish_options_for_sensor(sensor, name)
        for filter in self.filters:
            name = filter['name']
            self.publish_options_for_sensor(filter['filter'], name)
        print 'Done publishing dynamic options'

    def publish_options_for_sensor(self, sensor, name):
        self.ddynrec[name] = DDynamicReconfigure('_'.join(name.split()))
        for option_m in rs.option.__members__.items():
            option_str, option = option_m
            if not sensor.supports(option) or sensor.is_option_read_only(option):
                continue
            if self.is_checkbox(sensor, option):
                self.ddynrec[name].add_variable(option_str, sensor.get_option_description(option), bool(sensor.get_option(option)))
                continue
            enum_list = self.get_enum_list(sensor, option)
            if enum_list:
                option_name = str(option).split('.')[1]
                enum_method = self.get_enum_method(self.ddynrec[name], enum_list, option_name)
                self.ddynrec[name].add_variable(option_str, sensor.get_option_description(option),
                                                int(sensor.get_option(option)),
                                                int(sensor.get_option_range(option).min),
                                                int(sensor.get_option_range(option).max),
                                                edit_method=enum_method)
            else:
                self.ddynrec[name].add_variable(option_str, sensor.get_option_description(option),
                                                sensor.get_option(option),
                                                sensor.get_option_range(option).min,
                                                sensor.get_option_range(option).max)

        # self.ddynrec[name].dyn_rec_srv = Server(self.ddynrec[name].get_type(), self.create_callback(sensor, name), namespace='_'.join(name.split()))
        # import pdb; pdb.set_trace()
        if self.ddynrec[name].get_variable_names():
            self.ddynrec[name].start(self.create_callback(sensor, name))


    def is_checkbox(self, sensor, option):
        op_range = sensor.get_option_range(option)
        return (op_range.min == 0 and op_range.max == 1 and op_range.step == 1)

    def is_enum_option(self, sensor, option):
        op_range = sensor.get_option_range(option)
        if op_range.step != 1.0:
            return False
        if op_range.min < 0 or op_range.max > 1000:
            return False
        for val in range(int(op_range.min), int(op_range.max + 1), int(op_range.step)):
            if sensor.get_option_value_description(option, val) is None:
                return False
        return True

    def get_enum_list(self, sensor, option):
        str_list = []
        if self.is_enum_option(sensor, option):
            op_range = sensor.get_option_range(option)
            for val in range(int(op_range.min), int(op_range.max+1), int(op_range.step)):
                val_str = sensor.get_option_value_description(option, val)
                str_list.append([val_str, int(val)])

        return str_list

    def get_enum_method(self, ddynrec, enum_list, option_name):
        str_list = [ddynrec.const(item[0], "int", item[1], item[0]) for item in enum_list]
        enum_method = ddynrec.enum(str_list, option_name)
        return enum_method


    def get_stereo_baseline(self):
        return self.pipeline.get_active_profile().get_device().first_depth_sensor().get_option(rs.option.stereo_baseline)

    def set_initial_params(self):
        self.base_frame_id = rospy.get_param('~base_frame_id', constants.DEFAULT_BASE_FRAME_ID)
        self.frame_id = \
            {(rs.stream.depth, 0, rs.format.z16): rospy.get_param('~depth_frame_id', constants.DEFAULT_DEPTH_FRAME_ID),
             (rs.stream.infrared, 1, rs.format.y8): rospy.get_param('~infra1_frame_id', constants.DEFAULT_INFRA1_FRAME_ID),
             (rs.stream.infrared, 2, rs.format.y8): rospy.get_param('~infra2_frame_id', constants.DEFAULT_INFRA2_FRAME_ID),
             (rs.stream.color, 0, rs.format.rgb8): rospy.get_param('~color_frame_id', constants.DEFAULT_COLOR_FRAME_ID),
             (rs.stream.fisheye, 0, rs.format.raw8): rospy.get_param('~fisheye_frame_id', constants.DEFAULT_FISHEYE_FRAME_ID),
             (rs.stream.gyro, 0, rs.format.motion_xyz32f): rospy.get_param('~imu_gyro_frame_id', constants.DEFAULT_IMU_FRAME_ID),
             (rs.stream.accel, 0, rs.format.motion_xyz32f): rospy.get_param('~imu_accel_frame_id', constants.DEFAULT_IMU_FRAME_ID),
             }

        self.optical_frame_id = \
            {(rs.stream.depth, 0, rs.format.z16): rospy.get_param('~depth_optical_frame_id', constants.DEFAULT_DEPTH_OPTICAL_FRAME_ID),
             (rs.stream.infrared, 1, rs.format.y8): rospy.get_param('~infra1_optical_frame_id', constants.DEFAULT_INFRA1_OPTICAL_FRAME_ID),
             (rs.stream.infrared, 2, rs.format.y8): rospy.get_param('~infra2_optical_frame_id', constants.DEFAULT_INFRA2_OPTICAL_FRAME_ID),
             (rs.stream.color, 0, rs.format.rgb8): rospy.get_param('~color_optical_frame_id', constants.DEFAULT_COLOR_OPTICAL_FRAME_ID),
             (rs.stream.fisheye, 0, rs.format.raw8): rospy.get_param('~fisheye_optical_frame_id', constants.DEFAULT_FISHEYE_OPTICAL_FRAME_ID),
             (rs.stream.gyro, 0, rs.format.motion_xyz32f): rospy.get_param('~gyro_optical_frame_id', constants.DEFAULT_GYRO_OPTICAL_FRAME_ID),
             (rs.stream.accel, 0, rs.format.motion_xyz32f): rospy.get_param('~accel_optical_frame_id', constants.DEFAULT_ACCEL_OPTICAL_FRAME_ID),
             }

        self.depth_aligned_frame_id = \
            {(rs.stream.color, 0, rs.format.rgb8): rospy.get_param('~aligned_depth_to_color_frame_id', constants.DEFAULT_ALIGNED_DEPTH_TO_COLOR_FRAME_ID),
             (rs.stream.infrared, 1, rs.format.y8): rospy.get_param('~aligned_depth_to_infra1_frame_id', constants.DEFAULT_ALIGNED_DEPTH_TO_INFRA1_FRAME_ID),
             (rs.stream.infrared, 2, rs.format.y8): rospy.get_param('~aligned_depth_to_infra2_frame_id', constants.DEFAULT_ALIGNED_DEPTH_TO_INFRA2_FRAME_ID),
             (rs.stream.fisheye, 0, rs.format.raw8): rospy.get_param('~aligned_depth_to_fisheye_frame_id', constants.DEFAULT_ALIGNED_DEPTH_TO_FISHEYE_FRAME_ID),
             }

    def get_frame_id(self, normal_dict, pr):
        try:
            frame_id = normal_dict[(pr.stream_type(), pr.stream_index(), pr.format())]
        except:
            frame_id = normal_dict[(pr.stream_type(), pr.stream_index(), stream_to_format[pr.stream_type()])] + '_' + format_to_cvtype[pr.format()]
        return frame_id

    def get_camera_info(self, frame):
        video_profile = frame.as_video_frame().get_profile()
        intrinsic = video_profile.as_video_stream_profile().get_intrinsics()

        cam_info = sensor_msgs.msg.CameraInfo()

        cam_info.width = intrinsic.width
        cam_info.height = intrinsic.height
        cam_info.header.frame_id = self.get_frame_id(self.optical_frame_id, video_profile)
        cam_info.K = [intrinsic.fx, 0.0, intrinsic.ppx,
                      0.0, intrinsic.fy, intrinsic.ppy,
                      0.0, 0.0, 1.0]

        cam_info.P = [intrinsic.fx, 0.0, intrinsic.ppx, 0.0,
                      0.0, intrinsic.fy, intrinsic.ppy, 0.0,
                      0.0, 0.0, 1.0, 0.0]
        # if video_profile.stream_type == rs.stream.depth and video_profile.stream_index == 2:
        #     cam_info.P[3] = (-cam_info.P[3]) * self.get_stereo_baseline()

        cam_info.distortion_model = "plumb_bob"

        cam_info.R = [1.0, 0.0, 0.0,
                      0.0, 1.0, 0.0,
                      0.0, 0.0, 1.0]

        cam_info.D = intrinsic.coeffs

        # sensor = self.pipeline.get_active_profile().get_device().sensors[0]
        # frames[2].profile.get_extrinsics_to(frames[1].profile)


    def get_pipeline(self):
        pipeline = rs.pipeline()
        cfg = rs.config()
        if rospy.get_param('~rosbag_filename', ''):
            rosbag_filename = rospy.get_param('~rosbag_filename', '')
            rospy.loginfo('Running from file: %s' % rosbag_filename)
            cfg.enable_device_from_file(rosbag_filename, False);
            cfg.enable_all_streams();
        else:
            serial_no = rospy.get_param('~serial_no', '')
            cfg.enable_device(serial_no)
            #  pipeline.get_active_profile().get_device().get_info(rs.camera_info.serial_number)
            if rospy.get_param('~enable_color', False):
                cfg.enable_stream(rs.stream.color, 0, rospy.get_param('~color_width', 0), rospy.get_param('~color_height', 0), rs.format.any, rospy.get_param('~color_fps', '0'))
            if rospy.get_param('~enable_depth', False):
                cfg.enable_stream(rs.stream.depth, 0, rospy.get_param('~depth_width', 0), rospy.get_param('~depth_height', 0), rs.format.any, rospy.get_param('~depth_fps', '0'))
            if rospy.get_param('~enable_infra1', False):
                cfg.enable_stream(rs.stream.infrared, 1, rospy.get_param('~infra1_width', 0), rospy.get_param('~infra1_height', 0), rs.format.any, rospy.get_param('~infra1_fps', 0))
            if rospy.get_param('~enable_infra2', False):
                cfg.enable_stream(rs.stream.infrared, 2, rospy.get_param('~infra2_width', 0), rospy.get_param('~infra2_height', 0), rs.format.any, rospy.get_param('~infra2_fps', 0))
            rospy.loginfo('Connecting to camera %s' % ('...' if serial_no == '' else ' with serial number %s' % serial_no))
        pipeline.start(cfg)
        rospy.loginfo('Done.')
        return pipeline

    def get_frame_key(self, frame):
        pr = frame.get_profile()
        return '%s,%s,%s' % (pr.stream_type(), pr.stream_index(), pr.format())

    def start_image_publisher(self, sample_frame):
        pr = sample_frame.get_profile()
        name = type_to_name[pr.stream_type()] + (str(pr.stream_index()) if pr.stream_index() > 0 else '')
        rect = 'rect_' if pr.stream_type() in [rs.stream.depth, rs.stream.infrared] else ''
        format_str = 'raw' if stream_to_format[pr.stream_type()] == pr.format() else format_to_cvtype[pr.format()]
        topic_name = '%s/image_%s%s' % (name, rect, format_str)
        return rospy.Publisher(topic_name, sensor_msgs.msg.Image, queue_size=1)

    def start_info_publisher(self, sample_frame):
        pr = sample_frame.get_profile()
        name = type_to_name[pr.stream_type()] + (str(pr.stream_index()) if pr.stream_index() > 0 else '')
        topic_name = '%s/camera_info' % name
        return rospy.Publisher(topic_name, sensor_msgs.msg.CameraInfo, queue_size=1)


    def publish_frame(self, frame, crnt_time):
        kk = self.get_frame_key(frame)
        seq = self.seq.setdefault(kk, 0)
        seq += 1
        publisher = self.publishers.setdefault(kk + '_img', self.start_image_publisher(frame))
        if publisher.get_num_connections():
            pr = frame.get_profile()
            imgc = self.bridge.cv2_to_imgmsg(np.asarray(frame.get_data()), format_to_cvtype[pr.format()])
            imgc.header.frame_id = self.get_frame_id(self.optical_frame_id, pr)
            imgc.header.stamp = crnt_time
            imgc.header.seq = seq
            publisher.publish(imgc)

        publisher = self.publishers.setdefault(kk + '_info', self.start_info_publisher(frame))
        if publisher.get_num_connections():
            cam_info = self.camera_info.setdefault(kk, self.get_camera_info(frame))
            cam_info.header.stamp = crnt_time
            cam_info.header.seq = seq
            publisher.publish(cam_info)

    def run(self):
        rospy.loginfo('Started listening for frames.')
        while not rospy.is_shutdown():
            frames = self.pipeline.wait_for_frames()
            # rospy.loginfo('Got frames:')
            # print '\n'.join(['type: %s, index: %d, format %s' % (frame.get_profile().stream_type(), frame.get_profile().stream_index(), frame.get_profile().format) for frame in frames])
            crnt_time = rospy.Time.now()
            for filter in self.filters:
                frames = filter['filter'].process(frames).as_frameset()

            for frame in frames:

                if frame.is_points():
                    rospy.logerror('publish pointcloud is not implemented.')
                else:
                    self.publish_frame(frame, crnt_time)
            # [[frame.get_profile().stream_type(), frame.get_profile().stream_index()] for frame in frames]


def init():
    rospy.init_node('rs2_camera', anonymous=True)
    ros_rs2 = rs2_wrapper()
    ros_rs2.run()

