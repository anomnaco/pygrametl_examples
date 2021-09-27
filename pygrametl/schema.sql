begin;

-- schema
drop schema if exists pygrametlexa cascade;
create schema pygrametlexa;
set search_path to pygrametlexa;


-- tables
create table topleveldomain(
   topleveldomainid int,
   topleveldomain varchar
);

create table domain(
   domainid int,
   domain varchar,
   topleveldomainid int
);


create table server(
   serverid int,
   server varchar
);

create table serverversion(
   serverversionid int,
   serverversion varchar,
   serverid int
);

create table page(
   pageid int,
   url varchar,
   size int,
   validfrom date,
   validto date,
   version int,
   domainid int,
   serverversionid int
);

create table test(
   testid int,
   testname varchar,
   testauthor varchar
);
insert into test values (0, 'Test0', 'Joe Doe'), (1, 'Test1', 'Joe Doe'), 
(2, 'Test2', 'Joe Doe'), (3, 'Test3', 'Joe Doe'), (4, 'Test4', 'Joe Doe'),
(-1, 'Unknown test', 'Unknown author');


create table date(
   dateid int,
   date date,
   day int,
   month int,
   year int,
   week int,
   weekyear int
);

create table testresults(
   pageid int,
   testid int,
   dateid int,
   errors int
);


-- primary keys
alter table topleveldomain add primary key(topleveldomainid);
alter table domain add primary key(domainid);
alter table server add primary key(serverid);
alter table serverversion add primary key(serverversionid);
alter table page add primary key(pageid);
alter table test add primary key(testid);
alter table date add primary key(dateid);
alter table testresults add primary key(pageid, testid, dateid);


-- foreign keys
-- alter table domain add foreign key(topleveldomainid) references topleveldomain;
-- alter table serverversion add foreign key(serverid) references server;
-- alter table page add foreign key(domainid) references domain, 
--   add foreign key(serverversionid) references serverversion;
-- alter table testresults add foreign key(pageid) references page,
--   add foreign key(testid) references test,
--   add foreign key(dateid) references date;


-- indexes

create index url_version_idx on page(url, version desc);

commit;