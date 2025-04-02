# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from values
# MAGIC ('England & Wales', '2023-01-02', 'New Year’s Day (substitute day)'                         ),
# MAGIC ('England & Wales', '2023-04-07', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2023-04-10', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2023-05-01', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2023-05-08', 'Bank holiday for the coronation of King Charles III'     ),
# MAGIC ('England & Wales', '2023-05-29', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2023-08-28', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2023-12-25', 'Christmas Day'                                           ),
# MAGIC ('England & Wales', '2023-12-26', 'Boxing Day'                                              ),
# MAGIC ('England & Wales', '2024-01-01', 'New Year’s Day'                                          ),
# MAGIC ('England & Wales', '2024-03-29', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2024-04-01', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2024-05-06', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2024-05-27', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2024-08-26', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2024-12-25', 'Christmas Day'                                           ),
# MAGIC ('England & Wales', '2024-12-26', 'Boxing Day'                                              ),
# MAGIC ('England & Wales', '2025-01-01', 'New Year’s Day'                                          ),
# MAGIC ('England & Wales', '2025-04-18', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2025-04-21', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2025-05-05', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2025-05-26', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2025-08-25', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2025-12-25', 'Christmas Day'                                           ),
# MAGIC ('England & Wales', '2025-12-26', 'Boxing Day'                                              ),
# MAGIC ('England & Wales', '2026-01-01', 'New Year’s Day'                                          ),
# MAGIC ('England & Wales', '2026-04-03', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2026-04-06', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2026-05-04', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2026-05-25', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2026-08-31', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2026-12-25', 'Christmas Day'                                           ),
# MAGIC ('England & Wales', '2026-12-26', 'Boxing Day'                                              ),
# MAGIC ('England & Wales', '2022-01-03', 'New Year’s Day (substitute day)'                         ),
# MAGIC ('England & Wales', '2022-04-15', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2022-04-18', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2022-05-02', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2022-06-02', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2022-06-03', 'Platinum Jubilee bank holiday'                           ),
# MAGIC ('England & Wales', '2022-08-29', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2022-09-19', 'Bank Holiday for the State Funeral of Queen Elizabeth II'),
# MAGIC ('England & Wales', '2022-12-26', 'Boxing Day'                                              ),
# MAGIC ('England & Wales', '2022-12-27', 'Christmas Day (substitute day)'                          ),
# MAGIC ('England & Wales', '2021-01-01', 'New Year’s Day'                                          ),
# MAGIC ('England & Wales', '2021-04-02', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2021-04-05', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2021-05-03', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2021-05-31', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2021-08-30', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2021-12-27', 'Christmas Day (substitute day)'                          ),
# MAGIC ('England & Wales', '2021-12-28', 'Boxing Day (substitute day)'                             ),
# MAGIC ('England & Wales', '2020-04-01', 'New Year’s Day'                                          ),
# MAGIC ('England & Wales', '2020-04-10', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2020-04-13', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2020-05-08', 'Early May bank holiday (VE day)'                         ),
# MAGIC ('England & Wales', '2020-05-25', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2020-08-31', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2020-12-25', 'Christmas Day'                                           ),
# MAGIC ('England & Wales', '2020-12-28', 'Boxing Day (substitute day)'                             ),
# MAGIC ('England & Wales', '2019-01-01', 'New Year’s Day'                                          ),
# MAGIC ('England & Wales', '2019-04-19', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2019-04-22', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2019-05-06', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2019-05-27', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2019-08-26', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2019-12-25', 'Christmas Day'                                           ),
# MAGIC ('England & Wales', '2019-12-26', 'Boxing Day'                                              ),
# MAGIC ('England & Wales', '2018-01-01', 'New Year’s Day'                                          ),
# MAGIC ('England & Wales', '2018-03-30', 'Good Friday'                                             ),
# MAGIC ('England & Wales', '2018-04-02', 'Easter Monday'                                           ),
# MAGIC ('England & Wales', '2018-05-07', 'Early May bank holiday'                                  ),
# MAGIC ('England & Wales', '2018-05-28', 'Spring bank holiday'                                     ),
# MAGIC ('England & Wales', '2018-08-27', 'Summer bank holiday'                                     ),
# MAGIC ('England & Wales', '2018-12-25', 'Christmas Day'                                           ),
# MAGIC ('England & Wales', '2018-12-26', 'Boxing Day'                                              )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS start_date;
# MAGIC DECLARE VARIABLE start_date date DEFAULT cast('2020-01-01' as date);
# MAGIC
# MAGIC with cte_bankhols as
# MAGIC (
# MAGIC   select col1 as string, cast(col2 as date) as date_key, cast(col3 as string) bank_holiday
# MAGIC   from values
# MAGIC   ('England & Wales', '2023-01-02', 'New Year’s Day (substitute day)'                         ),
# MAGIC   ('England & Wales', '2023-04-07', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2023-04-10', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2023-05-01', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2023-05-08', 'Bank holiday for the coronation of King Charles III'     ),
# MAGIC   ('England & Wales', '2023-05-29', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2023-08-28', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2023-12-25', 'Christmas Day'                                           ),
# MAGIC   ('England & Wales', '2023-12-26', 'Boxing Day'                                              ),
# MAGIC   ('England & Wales', '2024-01-01', 'New Year’s Day'                                          ),
# MAGIC   ('England & Wales', '2024-03-29', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2024-04-01', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2024-05-06', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2024-05-27', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2024-08-26', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2024-12-25', 'Christmas Day'                                           ),
# MAGIC   ('England & Wales', '2024-12-26', 'Boxing Day'                                              ),
# MAGIC   ('England & Wales', '2025-01-01', 'New Year’s Day'                                          ),
# MAGIC   ('England & Wales', '2025-04-18', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2025-04-21', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2025-05-05', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2025-05-26', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2025-08-25', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2025-12-25', 'Christmas Day'                                           ),
# MAGIC   ('England & Wales', '2025-12-26', 'Boxing Day'                                              ),
# MAGIC   ('England & Wales', '2026-01-01', 'New Year’s Day'                                          ),
# MAGIC   ('England & Wales', '2026-04-03', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2026-04-06', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2026-05-04', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2026-05-25', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2026-08-31', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2026-12-25', 'Christmas Day'                                           ),
# MAGIC   ('England & Wales', '2026-12-26', 'Boxing Day'                                              ),
# MAGIC   ('England & Wales', '2022-01-03', 'New Year’s Day (substitute day)'                         ),
# MAGIC   ('England & Wales', '2022-04-15', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2022-04-18', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2022-05-02', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2022-06-02', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2022-06-03', 'Platinum Jubilee bank holiday'                           ),
# MAGIC   ('England & Wales', '2022-08-29', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2022-09-19', 'Bank Holiday for the State Funeral of Queen Elizabeth II'),
# MAGIC   ('England & Wales', '2022-12-26', 'Boxing Day'                                              ),
# MAGIC   ('England & Wales', '2022-12-27', 'Christmas Day (substitute day)'                          ),
# MAGIC   ('England & Wales', '2021-01-01', 'New Year’s Day'                                          ),
# MAGIC   ('England & Wales', '2021-04-02', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2021-04-05', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2021-05-03', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2021-05-31', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2021-08-30', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2021-12-27', 'Christmas Day (substitute day)'                          ),
# MAGIC   ('England & Wales', '2021-12-28', 'Boxing Day (substitute day)'                             ),
# MAGIC   ('England & Wales', '2020-04-01', 'New Year’s Day'                                          ),
# MAGIC   ('England & Wales', '2020-04-10', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2020-04-13', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2020-05-08', 'Early May bank holiday (VE day)'                         ),
# MAGIC   ('England & Wales', '2020-05-25', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2020-08-31', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2020-12-25', 'Christmas Day'                                           ),
# MAGIC   ('England & Wales', '2020-12-28', 'Boxing Day (substitute day)'                             ),
# MAGIC   ('England & Wales', '2019-01-01', 'New Year’s Day'                                          ),
# MAGIC   ('England & Wales', '2019-04-19', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2019-04-22', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2019-05-06', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2019-05-27', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2019-08-26', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2019-12-25', 'Christmas Day'                                           ),
# MAGIC   ('England & Wales', '2019-12-26', 'Boxing Day'                                              ),
# MAGIC   ('England & Wales', '2018-01-01', 'New Year’s Day'                                          ),
# MAGIC   ('England & Wales', '2018-03-30', 'Good Friday'                                             ),
# MAGIC   ('England & Wales', '2018-04-02', 'Easter Monday'                                           ),
# MAGIC   ('England & Wales', '2018-05-07', 'Early May bank holiday'                                  ),
# MAGIC   ('England & Wales', '2018-05-28', 'Spring bank holiday'                                     ),
# MAGIC   ('England & Wales', '2018-08-27', 'Summer bank holiday'                                     ),
# MAGIC   ('England & Wales', '2018-12-25', 'Christmas Day'                                           ),
# MAGIC   ('England & Wales', '2018-12-26', 'Boxing Day'                                              )
# MAGIC ),
# MAGIC cte_calendar as 
# MAGIC (
# MAGIC   select
# MAGIC     dateadd(start_date, seq) as date_key,
# MAGIC     if(month(dateadd(start_date, seq)) < 7, 1, 2) as half_year,
# MAGIC     if(month(dateadd(start_date, seq)) < 7, 'H1', 'H2') as half_year_name_short,
# MAGIC     if(month(dateadd(start_date, seq)) < 7, '1st half', '2nd half') as half_year_name
# MAGIC   from
# MAGIC   (
# MAGIC     select row_number() over (order by 1) as seq
# MAGIC     from
# MAGIC     (
# MAGIC       select 1 as r
# MAGIC       from system.information_schema.columns,
# MAGIC       system.information_schema.columns
# MAGIC       limit 20000
# MAGIC     ) generator
# MAGIC   ) iterator
# MAGIC ),
# MAGIC cte_working_calendar as
# MAGIC (
# MAGIC   select
# MAGIC     c.date_key,
# MAGIC     c.date_key                                       as date,
# MAGIC     date_format(c.date_key, 'MM-dd')                 as month_day,
# MAGIC     cast(date_format(c.date_key, 'D') as int)        as day_of_month,
# MAGIC     date_format(c.date_key, 'MMM-E')                 as month_day_name_short,
# MAGIC     date_format(c.date_key, 'MMMM-E')                as month_day_name,
# MAGIC     date_format(c.date_key, 'yyyy-D')                as year_day,
# MAGIC     date_format(c.date_key, 'D')                     as day_of_year,
# MAGIC     date_format(c.date_key, 'yyyy-E')                as year_day_name,
# MAGIC     date_format(c.date_key, 'E')                     as day_name_short,
# MAGIC     date_format(c.date_key, 'EEEE')                  as day_name,
# MAGIC     -- month
# MAGIC     cast(date_format(c.date_key, 'yyyyMM') as int)   as month_key,
# MAGIC     date_format(c.date_key, 'yyyy-MM')               as year_month,
# MAGIC     date_format(c.date_key, 'M')                     as month_of_year,
# MAGIC     date_format(c.date_key, 'yyyy-MMM')              as year_month_name,
# MAGIC     date_format(c.date_key, 'MMM')                   as month_name_short,
# MAGIC     date_format(c.date_key, 'MMMM')                  as month_name,
# MAGIC     -- quarter  
# MAGIC     cast(date_format(c.date_key, 'yyyyQQ') as int)   as quarter_key,
# MAGIC     date_format(c.date_key, 'yyyy-QQ')               as year_quarter,
# MAGIC     date_format(c.date_key, 'Q')                     as quarter_of_year,
# MAGIC     date_format(c.date_key, 'yyyy-QQQ')              as year_quarter_name,
# MAGIC     date_format(c.date_key, 'QQQ')                   as quarter_name_short,
# MAGIC     date_format(c.date_key, 'QQQQ')                  as quarter_name,
# MAGIC     -- half  
# MAGIC     cast(concat(
# MAGIC       date_format(c.date_key, 'yyyy'), 
# MAGIC       c.half_year) as int)                           as half_key,
# MAGIC     concat(
# MAGIC       date_format(c.date_key, 'yyyy'), 
# MAGIC       '-0', c.half_year)                             as year_half,
# MAGIC     c.half_year,
# MAGIC     concat(date_format(c.date_key, 'yyyy'),
# MAGIC       '-', c.half_year_name_short)                   as year_half_name,
# MAGIC     c.half_year_name_short,
# MAGIC     c.half_year_name,
# MAGIC     -- year  
# MAGIC     cast(date_format(c.date_key, 'yyyy') as int)     as `year`,
# MAGIC     -- flags
# MAGIC     b.date_key is not null                           as is_bank_holiday,
# MAGIC     (b.date_key is null and 
# MAGIC     !date_format(c.date_key, 'E') in ('Sat', 'Sun')) as is_working_day
# MAGIC   from cte_calendar c
# MAGIC   left join cte_bankhols b on c.date_key = b.date_key
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC     `date_key`,
# MAGIC     `date`,
# MAGIC     `month_day`,
# MAGIC     `day_of_month`,
# MAGIC     if(
# MAGIC       `is_working_day`,
# MAGIC       sum(cast(`is_working_day` as int)) 
# MAGIC         over (partition by `month_key` order by `date_key`),
# MAGIC       null
# MAGIC     ) as `working_day_of_month`,
# MAGIC     `month_day_name_short`,
# MAGIC     `month_day_name`,
# MAGIC     `year_day`,
# MAGIC     `day_of_year`,
# MAGIC     if(
# MAGIC       `is_working_day`,
# MAGIC       sum(cast(`is_working_day` as int)) 
# MAGIC         over (partition by `year` order by `date_key`),
# MAGIC       null
# MAGIC     ) as `working_day_of_year`,
# MAGIC     `year_day_name`,
# MAGIC     `day_name_short`,
# MAGIC     `day_name`,
# MAGIC     -- month
# MAGIC     `month_key`,
# MAGIC     `year_month`,
# MAGIC     `month_of_year`,
# MAGIC     if(
# MAGIC       `is_working_day`,
# MAGIC       sum(cast(`is_working_day` as int)) 
# MAGIC         over (partition by `quarter_key` order by `date_key`),
# MAGIC       null
# MAGIC     ) as `working_day_of_quarter`,
# MAGIC     `year_month_name`,
# MAGIC     `month_name_short`,
# MAGIC     `month_name`,
# MAGIC     -- quarter  
# MAGIC     `quarter_key`,
# MAGIC     `year_quarter`,
# MAGIC     `quarter_of_year`,
# MAGIC     `year_quarter_name`,
# MAGIC     `quarter_name_short`,
# MAGIC     `quarter_name`,
# MAGIC     -- half  
# MAGIC     `half_key`,
# MAGIC     `year_half`,
# MAGIC     `half_year`,
# MAGIC     `year_quarter_name`,
# MAGIC     `half_year_name_short`,
# MAGIC     `half_year_name`,
# MAGIC     -- year  
# MAGIC     `year`,
# MAGIC     -- flags
# MAGIC     `is_bank_holiday`,
# MAGIC     `is_working_day`,
# MAGIC     (`working_day_of_year`=1 and 
# MAGIC     `working_day_of_year` is not null)    as `is_1st_working_day_of_year`,
# MAGIC     (`working_day_of_month`=1 and 
# MAGIC     `working_day_of_month` is not null)   as `is_1st_working_day_of_month`,
# MAGIC     (`working_day_of_quarter`=1 and 
# MAGIC     `working_day_of_quarter` is not null) as `is_1st_working_day_of_quarter`,
# MAGIC     last_day(`date_key`) == `date_key`    as `is_last_day_of_month`,
# MAGIC     last_day(`date_key`) == `date_key` 
# MAGIC     and month(`date_key`) in (3,6,9,12)   as `is_last_day_of_quarter`
# MAGIC from cte_working_calendar
# MAGIC where date_key <= cast(concat(year(now()), '-12-31') as date)
# MAGIC
# MAGIC
