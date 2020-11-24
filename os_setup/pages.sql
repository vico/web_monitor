-- auto-generated definition
create table pages
(
    id              int auto_increment primary key,
    url             varchar(250) null,
    xpath           varchar(500) null,
    cron            varchar(250) null,
    text            mediumtext   null,
    diff            mediumtext   null,
    md5sum          varchar(32)  null,
    keyword         varchar(100) null,
    last_check_time datetime     null,
    updated_time    datetime     null,
    created_time    datetime     null
);

