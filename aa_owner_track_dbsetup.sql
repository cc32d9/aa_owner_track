CREATE DATABASE aa_owner_track;

CREATE USER 'aa_owner_track'@'localhost' IDENTIFIED BY 'Naul1iofae';
GRANT ALL ON aa_owner_track.* TO 'aa_owner_track'@'localhost';
grant SELECT on aa_owner_track.* to 'aa_owner_track_ro'@'%' identified by 'aa_owner_track_ro';

use aa_owner_track;

CREATE TABLE SYNC
(
 network           VARCHAR(15) PRIMARY KEY,
 block_num         BIGINT NOT NULL,
 block_time        DATETIME NOT NULL
) ENGINE=InnoDB;


