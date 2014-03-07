# --- Created by Slick DDL
# To stop Slick DDL generation, remove this comment and start using Evolutions

# --- !Ups

create table `user_registration` (`uid` BIGINT NOT NULL PRIMARY KEY,`myName` VARCHAR(254) NOT NULL,`organization_name` VARCHAR(254) NOT NULL,`organization_phone` VARCHAR(254) NOT NULL,`organization_email` VARCHAR(254) NOT NULL,`experience` INTEGER NOT NULL,`role` VARCHAR(254) NOT NULL,`why_participate` VARCHAR(254) NOT NULL,`what_tools` VARCHAR(254) NOT NULL);
create table `Sessions` (`Id` VARCHAR(254) NOT NULL PRIMARY KEY,`uid` BIGINT NOT NULL,`data` VARCHAR(254) NOT NULL,`timeStamp` BIGINT NOT NULL);
create table `user_info` (`uid` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,`given_name` VARCHAR(254) NOT NULL,`family_name` VARCHAR(254) NOT NULL,`email` VARCHAR(254) NOT NULL,`registered` BOOLEAN NOT NULL,`ipythonUrl` VARCHAR(254) NOT NULL,`cluster_id` VARCHAR(254),`secret` VARCHAR(254));
create table `white_list` (`uid` BIGINT NOT NULL PRIMARY KEY,`email` VARCHAR(254) NOT NULL);

# --- !Downs

drop table `user_registration`;
drop table `Sessions`;
drop table `user_info`;
drop table `white_list`;

