
---CREATING DIMENSION DIM_SAMPLE

CREATE TABLE DIM_SAMPLE
( SAMPLE_ID NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
SAMPLEVALID VARCHAR2(100 BYTE) NOT NULL ,
SAMPLESAMPLINGPOINT VARCHAR2(100 BYTE) NOT NULL ,
SAMPLESAMPLINGPOINTNOTATION VARCHAR2(100 BYTE) NOT NULL ,
SAMPLESAMPLINGPOINTLABEL VARCHAR2(50 BYTE) NOT NULL ,
SAMPLEISCOMPLIANCESAMPLE VARCHAR2(5 BYTE) NOT NULL,
SAMPLEPURPOSELABEL VARCHAR2(60 BYTE) NOT NULL,
SAMPLESAMPLINGPOINTEASTING NUMBER(8,2) NOT NULL,
SAMPLESAMPLINGPOINTNORTHING NUMBER(8,2) NOT NULL,
PRIMARY KEY ("SAMPLE_ID"));


---CREATING DIMENSION DIM_DETERMINANT 

CREATE TABLE DIM_DETERMINANT
( DETERMINANDNOTATION NUMBER NOT NULL ,
DETERMINANDLABEL VARCHAR2(15 BYTE) NOT NULL ,
DETERMINANDDEFINITION VARCHAR2(70 BYTE) NOT NULL ,
DETERMINANDUNITLABEL VARCHAR2(10 BYTE) NOT NULL ,
PRIMARY KEY ("DETERMINANDNOTATION"));


---CREATING DIMENSION TABLE DIM_TIME

CREATE TABLE DIM_TIME
( TIME_ID NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
SAMPLE_DATE DATE NOT NULL ,
TIME VARCHAR2(10 BYTE) NOT NULL,
WEEK NUMBER(4,0) NOT NULL ,
MONTH VARCHAR2(10 BYTE) NOT NULL ,
YEAR VARCHAR2(5 BYTE) NOT NULL ,
PRIMARY KEY ("TIME_ID"));

---CREATING FACT TABLE FACT_WATERSENSOR

CREATE TABLE FACT_WATERSENSOR
   (ID NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1), 
	SAMPLE_ID NUMBER NOT NULL , 
	TIME_ID NUMBER NOT NULL , 
	DETERMINANDNOTATION NUMBER(20,0) NOT NULL,
	SAMPLESAMPLEDMATERIALTYPELABEL VARCHAR2(70 BYTE), 	
	RESULT NUMBER NOT NULL, 
	PRIMARY KEY (ID),
	FOREIGN KEY (SAMPLE_ID) REFERENCES DIM_SAMPLE (SAMPLE_ID) ON DELETE CASCADE , 
	FOREIGN KEY (TIME_ID) REFERENCES DIM_TIME (TIME_ID) ON DELETE CASCADE , 
	FOREIGN KEY (DETERMINANDNOTATION) REFERENCES DIM_DETERMINANT (DETERMINANDNOTATION) ON DELETE CASCADE);


-----------------------------------------------------------------------------------

--- POPULATING dimension table DIM_SAMPLE FROM "WATERQUALITY"

declare
Cursor s_sample is         
select "@id","samplesamplingPoint","samplesamplingPointnotation","samplesamplingPointlabel","sampleisComplianceSample",
"samplepurposelabel","samplesamplingPointeasting","samplesamplingPointnorthing"
from "WATERQUALITY";
begin
for s_sam in s_sample loop
insert into DIM_SAMPLE(SAMPLEVALID,SAMPLESAMPLINGPOINT,SAMPLESAMPLINGPOINTNOTATION,SAMPLESAMPLINGPOINTLABEL,SAMPLEISCOMPLIANCESAMPLE       
,SAMPLEPURPOSELABEL,SAMPLESAMPLINGPOINTEASTING,SAMPLESAMPLINGPOINTNORTHING)
values(s_sam."@id",s_sam."samplesamplingPoint",s_sam."samplesamplingPointnotation",s_sam."samplesamplingPointlabel",DECODE(s_sam."sampleisComplianceSample",'0', 'FALSE', '1', 'TRUE'),
s_sam."samplepurposelabel",s_sam."samplesamplingPointeasting",s_sam."samplesamplingPointnorthing");
end loop;
end;
--------------------------------------------------

---POPULATING dimension table DIM_TIME  FROM "WATERQUALITY" 
declare
Cursor t_time is         
select "samplesampleDateTime"
from "WATERQUALITY";
dates DATE;
begin
for t_tim in t_time loop
dates := TO_DATE(t_tim."samplesampleDateTime", 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
insert into DIM_TIME(SAMPLE_DATE,TIME,WEEK,MONTH,YEAR)
values(dates,TO_CHAR(dates,'HH24:MI:SS'),to_number(to_char(dates,'WW')),TO_CHAR(dates,'MONTH'),TO_CHAR(dates,'YYYY'));
end loop;
end;
---------------------------------------------------------------
---POPULATING dimension table DIM_DETERMINANT FROM "WATERQUALITY" 

declare
Cursor d_deter is         
select distinct "determinandlabel","determinanddefinition","determinandnotation","determinandunitlabel"
from "WATERQUALITY";
begin
for d_det in d_deter loop
insert into DIM_DETERMINANT(DETERMINANDLABEL,DETERMINANDDEFINITION,DETERMINANDNOTATION,DETERMINANDUNITLABEL)
values(d_det."determinandlabel",d_det."determinanddefinition",d_det."determinandnotation",d_det."determinandunitlabel");
end loop;
end;
--------------------------------------------------
---POPULATING fact table FACT_WATERSENSOR

declare
Cursor fact_wat is         
select "ID",SAMPLE_ID,TIME_ID,DETERMINANDNOTATION,"samplesampledMaterialTypelabel","result" FROM "WATERQUALITY" w,DIM_SAMPLE s,DIM_TIME t,DIM_DETERMINANT d
WHERE W."ID" = S.SAMPLE_ID AND  W."ID" = T.TIME_ID AND W."determinandnotation" = D.DETERMINANDNOTATION; 
begin
for factw in fact_wat loop
insert into FACT_WATERSENSOR(SAMPLE_ID,TIME_ID,DETERMINANDNOTATION,SAMPLESAMPLEDMATERIALTYPELABEL,RESULT)
values(factw.SAMPLE_ID,factw.TIME_ID,factw.DETERMINANDNOTATION,factw."samplesampledMaterialTypelabel",factw."result");
end loop;
end;


------------------------------------------------
--STASTICAL QUERIES----

---1 The list of water sensors measured by type of it by month---------------
SELECT  d.DETERMINANDLABEL as SENSOR_TYPE,f.RESULT as MEASUREMENT,t.MONTH FROM fact_watersensor f,dim_time t,dim_determinant d  
where f.time_id = t.time_id and f.DETERMINANDNOTATION = d.DETERMINANDNOTATION order by d.DETERMINANDLABEL 


---2 The number of sensor measurements collected by type of sensor by week

SELECT d.DETERMINANDLABEL as sensor_type,t.WEEK as WEEK_OF_THE_YEAR,COUNT(f.RESULT) as No_of_sensor_measurements_by_week  FROM fact_watersensor f,dim_time t,dim_determinant d  
where f.time_id = t.time_id and f.DETERMINANDNOTATION = d.DETERMINANDNOTATION group by t.WEEK,d.DETERMINANDLABEL order by t.WEEK,d.DETERMINANDLABEL; 

---3 The number of measurements made by location by month

SELECT COUNT(f.RESULT) as sensor_measurement_COUNT ,s.SAMPLESAMPLINGPOINTLABEL as LOCATION,t.month FROM fact_watersensor f,dim_time t,dim_sample s  
where f.time_id = t.time_id and f.sample_id = s.sample_id group by t.month,s.SAMPLESAMPLINGPOINTLABEL order by s.SAMPLESAMPLINGPOINTLABEL; 

---4 The average number of measurements covered for PH by year

with ph_total as(
SELECT t.YEAR,COUNT(f.RESULT) as yearly_average   FROM fact_watersensor f,dim_time t,dim_determinant d 
where f.time_id = t.time_id and f.DETERMINANDNOTATION = d.DETERMINANDNOTATION and d.DETERMINANDLABEL = 'pH'  
group by t.YEAR),
overall as(SELECT t.YEAR,COUNT(f.RESULT) as yearly_average   FROM fact_watersensor f,dim_time t,dim_determinant d 
where f.time_id = t.time_id and f.DETERMINANDNOTATION = d.DETERMINANDNOTATION  
group by t.year)
select ph_total.year,ph_total.yearly_average as PH_COUNT,overall.yearly_average as REST_SENSOR_COUNT,round(((ph_total.yearly_average/overall.yearly_average)*100),2) as ph_sensor_avg from ph_total
join overall on ph_total.year=overall.year order by year;

---5 The average value of Nitrate measurements by locations by year

SELECT d.DETERMINANDLABEL as SENSOR_TYPE,s.SAMPLESAMPLINGPOINTLABEL as LOCATION,t.YEAR ,round(AVG(f.RESULT),2) as yearly_average  FROM fact_watersensor f,dim_time t,dim_sample s,dim_determinant d 
where f.time_id = t.time_id and f.sample_id = s.sample_id  and f.DETERMINANDNOTATION = d.DETERMINANDNOTATION and d.DETERMINANDLABEL = 'Nitrite-N'  
group by s.SAMPLESAMPLINGPOINTLABEL,t.year,d.DETERMINANDLABEL order by t.year;	
	


----- END-------


