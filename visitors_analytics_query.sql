--Visitor count by top 10 port cities  
SELECT port_city, count(admission_id) visitor_cnt
FROM visitor_analysis
GROUP BY port_city
ORDER BY visitor_cnt DESC
LIMIT 10;

--Visitor count by year, month, and gender
SELECT ad.year, ad.month, v.gender, count(va.admission_id) visitor_cnt
FROM visitor_analysis va
INNER JOIN visitors v
ON (va.admission_id = v.admission_id
    AND va.arrival_date = v.arrival_date)
INNER JOIN arrival_date ad
ON (va.arrival_date = ad.arrival_date)
GROUP BY ad.year, ad.month, v.gender;

--Visitor count by top 10 port cities and population 
SELECT va.port_city, sd.total_population, count(va.admission_id) visitor_cnt
FROM visitor_analysis va
LEFT JOIN state_demographics sd
ON (va.port_city = sd.city
    AND va.port_state_code = sd.state_code)
GROUP BY va.port_city, sd.total_population
ORDER BY visitor_cnt DESC
LIMIT 10;

--Visitor count by visa categories
SELECT visa_category, count(admission_id) visitor_cnt
FROM visitor_analysis
GROUP BY visa_category
ORDER BY visitor_cnt DESC;

--Visitor count by top 10 origin countries
SELECT origin_country, count(admission_id) visitor_cnt
FROM visitor_analysis
GROUP BY origin_country
ORDER BY visitor_cnt DESC
LIMIT 10;

