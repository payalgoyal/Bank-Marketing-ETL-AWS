-- 1. Deposits by Job Title
CREATE TABLE gold_job_vs_deposit AS
SELECT 
    job_title,
    COUNT(*) AS deposits
FROM silver_final
WHERE deposit = 'yes'
GROUP BY job_title
ORDER BY deposits DESC;

-- 2. Monthly Conversion Rate
CREATE TABLE gold_month_conversion AS
SELECT 
    month,
    COUNT(*) FILTER (WHERE deposit = 'yes') * 100.0 / COUNT(*) AS conversion_rate
FROM silver_final
GROUP BY month
ORDER BY 
    CASE month
        WHEN 'jan' THEN 1 WHEN 'feb' THEN 2 WHEN 'mar' THEN 3
        WHEN 'apr' THEN 4 WHEN 'may' THEN 5 WHEN 'jun' THEN 6
        WHEN 'jul' THEN 7 WHEN 'aug' THEN 8 WHEN 'sep' THEN 9
        WHEN 'oct' THEN 10 WHEN 'nov' THEN 11 WHEN 'dec' THEN 12
    END;

-- 3. Age Group vs Deposits
CREATE TABLE gold_age_group_perf AS
SELECT 
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 50 THEN '30–50'
        ELSE 'Above 50'
    END AS age_group,
    COUNT(*) AS deposits,
    ROUND(AVG(campaign), 2) AS avg_campaign_attempts
FROM silver_final
WHERE deposit = 'yes'
GROUP BY age_group;

-- 4. Loan Percentage by Education Level
CREATE TABLE gold_edu_loan_stats AS
SELECT 
    education_level,
    COUNT(*) FILTER (WHERE loan = 'yes') AS loans,
    COUNT(*) AS total,
    ROUND(COUNT(*) FILTER (WHERE loan = 'yes') * 100.0 / COUNT(*), 2) AS loan_percentage
FROM silver_final
GROUP BY education_level;