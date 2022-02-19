create database if not exists emeters_db;

use emeters_db;

create table if not exists meter_reading(
      mr_meter_sk int
,     mr_timestamp timestamp
,     mr_reading decimal(10,2)
)
partitioned by (mr_reading_date string);

create table if not exists customer(
      c_customer_sk int
,     c_salutation char(10)
,     c_first_name char(20)
,     c_last_name char(30)
,     c_birth_day
,     c_birth_month
,     c_birth_year
,     c_login char(13)
,     c_email_address char(50)
,     primary key (c_customer_sk) disable novalidate rely
);

create table if not exists meter(
      m_meter_sk int
,     m_meter_id char(32)
,     m_service_date date
,     m_region char(2)
,     m_status int
,     m_manufact char(50)
,     m_street_number
,     m_street_name
,     m_street_type
,     m_city
,     m_state
,     m_zip
,     m_country
,     primary key (m_meter_sk) disable novalidate rely
);
 

create table if not exists rates(
      r_region char(2)
,     r_rate decimal(7,2)
);

create table if not exists customer_meter(
      cm_meter_sk int
,     cm_customer_sk int
,     cm_status char(2)
,     cm_start_date date
,     cm_end_date date
,     foreign key  (cm_meter_sk) references meter (m_meter_sk) disable novalidate rely 
,     foreign key  (cm_customer_sk) references customer (c_customer_sk) disable novalidate rely 
);

create materialized view daily_reading            
partitioned on (mr_reading_date)  
clustered on (mr_meter_sk) 
as
select mr_reading_date, mr_meter_sk,  max(mr_reading) 
from meter_reading
group by mr_reading_date, mr_meter_sk;

create materialized view current_billing_cycle
as
select c_customer_sk, c_first_name, c_last_name, ca_street_number, ca_street_name, ca_street_type ,ca_suite_number,
ca_city, ca_county, ca_state, ca_zip, ca_country, cm_meter_sk, cm_start_date, cm_end_date, r_rate
from
customer_meter cm, customer c, customer_address ca, meter_reading mr, meter m, rates r
where
c.c_customer_sk = cm.cm_customer_sk
and c.c_current_addr_sk = ca.ca_address_sk
and cm.cm_meter_sk = m.m_meter_sk
and m.m_region = r.r_region
and m.m_status = 1
and (cm.cm_end_date is null or cm.cm_end_date > date_format(current_date,'yyyy-MM-01'));
