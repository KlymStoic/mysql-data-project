-- Exploratory Data Analysis

USE world_layoffs;

SELECT *
FROM layoffs_staging2;


/** Note: EDA is an open-ended approach to investigate data, uncovering trends,
		patterns, and outliers without a strict initial hypothesis;		
**/


SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;


SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Note: 1 means 100% of their company laid off;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Note: Through the funds column, we can see how big some of these companies were;


-- Companies with the biggest single layoffs:

SELECT company, total_laid_off
FROM layoffs_staging2
ORDER BY total_laid_off DESC
LIMIT 5;


-- Companies with the biggest total layoffs:

SELECT company, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off_sum DESC
LIMIT 10;


-- By country:

SELECT country, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY country
ORDER BY total_laid_off_sum DESC
LIMIT 10;


-- By location:

SELECT location, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY location
ORDER BY total_laid_off_sum DESC
LIMIT 10;


-- By years:

SELECT YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY years
ORDER BY years ASC;


-- By industry:

SELECT industry, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off_sum DESC
LIMIT 10;

-- Note: 'Consumer' & 'Retail' industries suffered the most;


-- By stage:

SELECT stage, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off_sum DESC
LIMIT 10;

/** Note: Stages represent a company's growth and funding journey,
		from early investment (Series B) to scaling (Series C-F),
		late-stage expansion (Series H, Private Equity), 
        going public (Post-IPO), or being Acquired;
**/


-- Now let's take a look per year:

WITH Company_Year AS (
SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY company, years 
), 
Company_Year_Rank AS (
SELECT 
	company, 
	years, 
	total_laid_off_sum,
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off_sum DESC) AS ranking
FROM Company_Year
)
SELECT
	company,
    years,
    total_laid_off_sum,
    ranking
FROM 
	Company_Year_Rank
WHERE ranking <= 3 
	AND years IS NOT NULL
ORDER BY years ASC, total_laid_off_sum DESC;


-- Rolling total per month:

SELECT 
    DATE_FORMAT(`date`, '%Y-%m') AS month_dates,
    SUM(total_laid_off) AS total_per_month
FROM
    layoffs_staging2
GROUP BY month_dates
ORDER BY month_dates ASC;

-- Now incorporate it into a CTE so we can query from it:

WITH monthly_total AS (
SELECT 
    DATE_FORMAT(`date`, '%Y-%m') AS month_dates,
    SUM(total_laid_off) AS total_per_month
FROM
    layoffs_staging2
GROUP BY month_dates
)
SELECT 
	month_dates,
	total_per_month, 
    SUM(total_per_month) OVER(ORDER BY month_dates) as rolling_total
FROM monthly_total
ORDER BY month_dates ASC;

/** Note: To extract the year and month from a date, the SUBSTRING function can be used.
		SUBSTRING(`date`, 1, 7) AS month_date;
**/

-- The End !@_@!