`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `received_delta_seconds`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `vision_result`: STRUCT<
      `image_data`: STRUCT<
        `image_location`: STRING,
        `file_name`: STRING,
        `capture_at_ms`: BIGINT,
        `camera_serial`: STRING,
        `image_resolution`: STRUCT<
          `x`: INT,
          `y`: INT
        >,
        `scale_factor`: DOUBLE,
        `image_uploaded`: BOOLEAN,
        `high_res_uploaded`: BOOLEAN,
        `file_extension_code`: INT
      >,
      `program_data`: STRUCT<
        `program_id`: BIGINT,
        `program_type`: INT
      >,
      `step_results`: ARRAY<
        STRUCT<
          `step_id`: INT,
          `result_code`: INT,
          `duration_ms`: BIGINT,
          `fixture_data`: STRUCT<
            `found`: BOOLEAN,
            `location`: STRUCT<
              `x`: INT,
              `y`: INT
            >,
            `match_milli_percent`: BIGINT,
            `rotation_degrees`: BIGINT,
            `translation`: STRUCT<
              `x`: INT,
              `y`: INT
            >,
            `feature_group`: STRUCT<
              `features`: ARRAY<
                STRUCT<
                  `contour`: STRUCT<
                    `points`: ARRAY<
                      STRUCT<
                        `x`: INT,
                        `y`: INT
                      >
                    >
                  >,
                  `key_points`: STRUCT<
                    `points`: ARRAY<
                      STRUCT<
                        `x`: DOUBLE,
                        `y`: DOUBLE,
                        `size`: DOUBLE,
                        `angle`: DOUBLE,
                        `response`: DOUBLE,
                        `octave`: BIGINT,
                        `class_id`: BIGINT
                      >
                    >,
                    `desc_mat`: STRUCT<
                      `rows`: BIGINT,
                      `cols`: BIGINT,
                      `mat_type`: BIGINT,
                      `data`: BINARY
                    >
                  >
                >
              >
            >,
            `binarization_threshold`: BIGINT
          >,
          `barcode_data`: ARRAY<
            STRUCT<
              `barcode_type`: INT,
              `contents`: STRING,
              `quality`: BIGINT,
              `barcode_boundary_points`: ARRAY<
                STRUCT<
                  `x`: INT,
                  `y`: INT
                >
              >,
              `localization_bounding_box_points`: ARRAY<
                STRUCT<
                  `x`: INT,
                  `y`: INT
                >
              >,
              `barcode_localization_time_ms`: BIGINT,
              `barcode_decoding_time_ms`: BIGINT,
              `matched`: BOOLEAN,
              `scanning_library`: INT,
              `barcode_rotation_angle`: BIGINT
            >
          >,
          `match_data`: STRUCT<
            `num_features`: BIGINT,
            `query_image_feature_count`: BIGINT,
            `total_num_matching_features`: BIGINT
          >,
          `histogram_average_color_data`: STRUCT<
            `matching_color_milli_percent`: BIGINT,
            `matching_color_pixel_count`: BIGINT
          >,
          `ocr_data`: STRUCT<
            `result`: STRING,
            `type`: INT,
            `match_date`: STRING,
            `trained_text_results`: ARRAY<
              STRUCT<
                `detected_char`: STRING,
                `font_index`: BIGINT,
                `bounding_box`: ARRAY<
                  STRUCT<
                    `x`: INT,
                    `y`: INT
                  >
                >,
                `match_percentage`: BIGINT,
                `segmented_character_base_64_image`: STRING
              >
            >
          >,
          `region_of_interest`: ARRAY<
            STRUCT<
              `x`: INT,
              `y`: INT
            >
          >,
          `find_circles_data`: STRUCT<
            `circles`: ARRAY<
              STRUCT<
                `center`: STRUCT<
                  `x`: INT,
                  `y`: INT
                >,
                `radius`: INT
              >
            >
          >,
          `find_rects_data`: STRUCT<
            `rects`: ARRAY<
              STRUCT<
                `points`: ARRAY<
                  STRUCT<
                    `x`: INT,
                    `y`: INT
                  >
                >,
                `area`: INT,
                `magnitude_sobel`: INT,
                `aspect_ratio`: DOUBLE,
                `angle_from_x_axis_degrees`: DOUBLE
              >
            >
          >,
          `find_blobs_data`: STRUCT<
            `blob`: ARRAY<
              STRUCT<
                `center`: STRUCT<
                  `x`: INT,
                  `y`: INT
                >,
                `area`: INT,
                `outline`: ARRAY<
                  STRUCT<
                    `x`: INT,
                    `y`: INT
                  >
                >,
                `circularity`: DOUBLE,
                `convexity`: DOUBLE,
                `inertia_ratio`: DOUBLE
              >
            >
          >,
          `distance_check_data`: STRUCT<
            `start`: STRUCT<
              `x`: INT,
              `y`: INT
            >,
            `end`: STRUCT<
              `x`: INT,
              `y`: INT
            >,
            `distance`: DOUBLE,
            `angle_from_x_axis_degrees`: DOUBLE
          >,
          `angle_check_data`: STRUCT<
            `edge_point_a`: STRUCT<
              `x`: INT,
              `y`: INT
            >,
            `edge_point_b`: STRUCT<
              `x`: INT,
              `y`: INT
            >,
            `angle_degrees`: DOUBLE,
            `interpolated_intersection`: STRUCT<
              `x`: INT,
              `y`: INT
            >
          >,
          `find_edge_data`: STRUCT<
            `points`: ARRAY<
              STRUCT<
                `x`: INT,
                `y`: INT
              >
            >,
            `angle_from_x_axis_degrees`: DOUBLE,
            `sharpness_milli_percent`: INT,
            `constrast_milli_percent`: INT,
            `straightness_milli_percent`: INT,
            `length_pixels`: INT,
            `all_edge_data`: ARRAY<
              STRUCT<
                `points`: ARRAY<
                  STRUCT<
                    `x`: INT,
                    `y`: INT
                  >
                >,
                `angle_from_x_axis_degrees`: DOUBLE,
                `sharpness_milli_percent`: INT,
                `constrast_milli_percent`: INT,
                `straightness_milli_percent`: INT,
                `length_pixels`: INT
              >
            >
          >,
          `color_match_data`: STRUCT<
            `hsv_color`: STRUCT<
              `hue`: INT,
              `saturation`: INT,
              `value`: INT
            >,
            `rgb_color`: STRUCT<
              `red`: INT,
              `green`: INT,
              `blue`: INT
            >,
            `gray_color`: INT
          >,
          `match_template_data`: STRUCT<
            `match`: ARRAY<
              STRUCT<
                `boundary_points`: ARRAY<
                  STRUCT<
                    `x`: INT,
                    `y`: INT
                  >
                >,
                `percentage`: BIGINT,
                `angle_degrees`: DOUBLE
              >
            >
          >,
          `polyline_data`: STRUCT<
            `boundary_points`: ARRAY<
              STRUCT<
                `x`: INT,
                `y`: INT
              >
            >,
            `match_percentage`: BIGINT,
            `angle_degrees`: BIGINT,
            `polyline_points`: ARRAY<
              STRUCT<
                `points`: ARRAY<
                  STRUCT<
                    `x`: INT,
                    `y`: INT
                  >
                >
              >
            >,
            `angle_from_x_axis_degrees`: DOUBLE
          >,
          `caliper_data`: STRUCT<
            `edge_data`: ARRAY<
              STRUCT<
                `points`: ARRAY<
                  STRUCT<
                    `x`: INT,
                    `y`: INT
                  >
                >,
                `angle_from_x_axis_degrees`: DOUBLE,
                `sharpness_milli_percent`: INT,
                `constrast_milli_percent`: INT,
                `straightness_milli_percent`: INT,
                `length_pixels`: INT,
                `all_edge_data`: ARRAY<
                  STRUCT<
                    `points`: ARRAY<
                      STRUCT<
                        `x`: INT,
                        `y`: INT
                      >
                    >,
                    `angle_from_x_axis_degrees`: DOUBLE,
                    `sharpness_milli_percent`: INT,
                    `constrast_milli_percent`: INT,
                    `straightness_milli_percent`: INT,
                    `length_pixels`: INT
                  >
                >
              >
            >,
            `distance`: DOUBLE,
            `distance_start_point`: STRUCT<
              `x`: INT,
              `y`: INT
            >,
            `distance_end_point`: STRUCT<
              `x`: INT,
              `y`: INT
            >
          >,
          `step_timed_out`: BOOLEAN
        >
      >,
      `overall_result`: INT,
      `inspection_ended_early`: BOOLEAN,
      `img_capture_to_log_time_duration_ms`: BIGINT,
      `img_capture_to_store_result_duration_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
