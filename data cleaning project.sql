-- Data Cleaning Project

USE world_layoffs;

SELECT * FROM layoffs;

/** Project plan:
1. Remove dublicates;
2. Standartize data;
3. Look after NULL or blank values;
4. Remove any columns/rows for optimization;
**/

-- Note: It's very bad to use raw table, so u need to create a staging table for experiments;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT * FROM layoffs_staging;


-- First Stage: Duplicates;

-- Query to find:

SELECT 
	*, 
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, 
		total_laid_off, percentage_laid_off, `date`, 
		stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
    
WITH duplicate_cte AS 
(
SELECT 
	*, 
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, 
		total_laid_off, percentage_laid_off, `date`, 
		stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

/** Note: Unfortunately in MySQL u can't use CTE to delete/update tables.
		Even if it looks legit like in that example:
**/

WITH duplicate_cte AS (
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
			total_laid_off, percentage_laid_off, `date`, 
            stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

-- Error Code: 1288. The target table duplicate_cte of the DELETE is not updatable;

-- Note: Because we don't have a unique column a.k.a index we need to make new table with that row_num column;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Note: To create 'layoffs_staging2' table I used 'Copy to Clipboard' -> 'Create Statement';

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
			total_laid_off, percentage_laid_off, `date`, 
            stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Note: To execute that query u need to disable 'Safe Updates' setting in 'SQL Editor';


-- Second stage: Standardizing data;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Note: 'TRIM' func removes blank spaces before and after in words;

-- Check:
SELECT * 
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

/** Note: We need to eliminate inconsistencies in naming.
		Otherwise, it will affect how we group industries in future graphical analyses;
**/

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 3 row(s) affected Rows matched: 102  Changed: 3  Warnings: 0

-- Check:
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Note: Every column worth of checking for duplicates like this);

SELECT * FROM layoffs_staging2;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Found an issue;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- OR

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Note: In the table the 'date' column seems to have a wrong data type;

SELECT 
	`date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
	layoffs_staging2;

-- Note: 'STR_TO_DATE' seems to work like python 'datetime.strptime' function;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- 2355 row(s) affected Rows matched: 2356  Changed: 2355  Warnings: 0

-- Note: To change datatype in the table u need to use 'ALTER TABLE' statement;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Note: We formatted 'date' column previously to avoid error message;


-- Third stage: Null and blank values;

SELECT *
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;
    
-- Note: We need to convert blank values into NULL, makes work less messy;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Check:
SELECT *
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;

SELECT *
FROM layoffs_staging2 st1
JOIN layoffs_staging2 st2
	ON st1.company = st2.company
WHERE st1.industry IS NULL
	AND st2.industry IS NOT NULL;
    
-- Note: Previously I tried joining tables with blank values and it was catastrophe;

UPDATE layoffs_staging2 st1
JOIN layoffs_staging2 st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE st1.industry IS NULL
	AND st2.industry IS NOT NULL;
    
-- Check:
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- Note: One is still left, but that company had no more layoffs, so we can't determine its industry;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


-- Fourth stage: Removing unnecessary columns/rows;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Note: We have to clean unusable data like this;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Check:
SELECT *
FROM layoffs_staging2;

-- Note: We don't need 'row_num' column anymore, so...

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
