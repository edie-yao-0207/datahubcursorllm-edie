CREATE TABLE `test_table_1` (
  `test_col_1` bigint(20) NOT NULL,
  `test_col_2` smallint(6) NOT NULL,
  `test_col_3` blob,
  `test_col_4` tinyint(1) NOT NULL,
  PRIMARY KEY (`test_col_1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `test_table_2` (
  `test_col_1` bigint(20) NOT NULL,
  `test_col_2` varchar(2083) NOT NULL,
  `test_col_3` blob,
  PRIMARY KEY (`test_col_1`, `test_col_2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `test_table_3` (
  `test_col_1` bigint(20) NOT NULL,
  `test_col_2` bigint(20) NOT NULL,
  KEY `col_1_id` (`test_col_1`)
  CONSTRAINT `fk_test_col_1_table_1` FOREIGN KEY (`test_col_1`) REFERENCES `test_table_1` (`test_col_1`) ON DELETE CASCADE
  CONSTRAINT `fk_test_col_2_table_1` FOREIGN KEY (`test_col_2`) REFERENCES `test_table_1` (`test_col_2`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `test_table_4` (
  `test_col_1` bigint(20) NOT NULL,
  `test_col_2` bigint(20) NOT NULL COMMENT "Column comments, like this one, can contain commas",
  `test_col_3` datetime(6) NOT NULL COMMENT 'in double or, like, single quotes',
  PRIMARY KEY (`test_col_1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
