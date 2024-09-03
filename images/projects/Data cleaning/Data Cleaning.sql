-- Data Cleaning --


SELECT *
FROM layoffs;


-- step 1: Remove Duplicates
-- step 2: Standardize the data
-- step 3: Remove Null values or blank values
-- step 4: Remove Any columns


#Creating an staging table, to keep the raw table safe.

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;


INSERT INTO layoffs_staging
SELECT * FROM layoffs;


-- Step 1: Removing Duplicates
#Partitioning with the column names. 
SELECT *,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

#By doing sub query getting to know the duplicates. which and all are greater than 2, those will be duplicates.
with duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

#Checking a value.
SELECT *
FROM layoffs_staging
where company = 'Casper';

#Creating a staging2 table to remove the duplicates. we cann't to delete the values from a cte, that's why we are creating a new table.
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

#Copying all the data from layoffs staging to staging2, for to delete the duplicates.
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date` , stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

#Deleting all the duplicates.
DELETE FROM layoffs_staging2
WHERE row_num >1;

#Checking
SELECT *
FROM layoffs_staging2
WHERE row_num <=1;



-- Step 2: Standardizing Data
#Usinng trim to make sure that there is no extra spaces
SELECT company,(TRIM(COMPANY))
FROM layoffs_staging2;

#Updating it.
UPDATE layoffs_staging2
SET company = TRIM(company);

#Checking the industry column has is there any same industry is different name.
SELECT distinct(industry)
FROM layoffs_staging2
order by 1;

#Updating it.
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

#Conforming that the name is corrected.
SELECT industry
FROM layoffs_staging2
WHERE industry = 'Crypto';

#Looking to location is there anything have to standardize.
SELECT distinct(location)
FROM layoffs_staging2
order by 1;

#Looking to location is there anything have to standardize. and found one
SELECT distinct(country)
FROM layoffs_staging2
order by 1;

#Updating it.
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

#Conforming that the name is corrected.
SELECT distinct(country)
FROM layoffs_staging2
order by 1;

#Checking the convertion of date.
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') as `date`
FROM layoffs_staging2;

#Updating.
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

#Checking
SELECT `date`
FROM layoffs_staging2;

#Modifying the date from text to date.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

#Checking
SELECT *
FROM layoffs_staging2;

#Checkking each column
SELECT DISTINCT(total_laid_off)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(percentage_laid_off)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(stage)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(funds_raised_millions)
FROM layoffs_staging2
ORDER BY 1;


-- Remove Null values or blank values

SELECT * FROM layoffs_staging2;

#Replacing blank values into null, only for industry.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

#checking
SELECT T1.industry, T2.industry
FROM layoffs_staging2 as T1
JOIN layoffs_staging2 as T2
	ON T1.company = T2.company
WHERE T1.industry IS NULL
AND T2.industry IS NOT NULL;

#updating
UPDATE layoffs_staging2 as T1
JOIN layoffs_staging2 as T2
	ON T1.company = T2.company
    SET T1.industry = T2.industry
WHERE T1.industry IS NULL
AND T2.industry IS NOT NULL;

#checking, now  it's perfect.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


#Here there are more columns containing NULL values but i can't to blindly delete those.
#They can contain also a null values.


-- Remove Any columns or rows
#Removing the column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

#Final Check
SELECT * FROM layoffs_staging2;