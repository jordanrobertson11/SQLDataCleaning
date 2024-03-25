CREATE TABLE nashville_housing_data (
	UniqueID INT,
	ParcelID VARCHAR(255),
	LandUse	VARCHAR(255),
	PropertyAddress	VARCHAR(255),
	SaleDate DATE,	
	SalePrice INT,
	LegalReference VARCHAR(255),
	SoldAsVacant VARCHAR(255),
	OwnerName VARCHAR(255),
	OwnerAddress VARCHAR(255),
	Acreage	DOUBLE,
	TaxDistrict	VARCHAR(255),
	LandValue INT,
	BuildingValue INT,
	TotalValue INT,
	YearBuilt INT,
	Bedrooms INT,
	FullBath INT,
	HalfBath INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Nashville Housing Data for Data Cleaning (reuploaded).csv' 
INTO TABLE nashville_housing_data
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SELECT * 
FROM nashville_housing_data;

-- ----------------------------------------------------------------------------------------------------

-- Populate property address data

SELECT *
FROM nashville_housing_data
-- WHERE PropertyAddress LIKE ""
ORDER BY ParcelID;

SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, IF(a.PropertyAddress = "", b.PropertyAddress, NULL)
FROM nashville_housing_data a
JOIN nashville_housing_data b
	ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress LIKE "";


UPDATE nashville_housing_data a
	JOIN nashville_housing_data b
	ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = IF(a.PropertyAddress = "", b.PropertyAddress, NULL)
WHERE a.PropertyAddress LIKE "";

-- ----------------------------------------------------------------------------------------------------

-- Breaking out address into individual columns (Address, City, State)

SELECT PropertyCity
FROM nashville_housing_data;
-- WHERE PropertyAddress LIKE ""
-- ORDER BY ParcelID;

SELECT REVERSE(SUBSTRING(REVERSE(PropertyAddress), 1, LOCATE(' ', REVERSE(PropertyAddress)) - 1)) AS City
FROM nashville_housing_data; -- Gets the last word in a string

SELECT RTRIM(REVERSE(SUBSTRING(REVERSE(PropertyAddress), LOCATE(' ', REVERSE(PropertyAddress))))) AS Address
FROM nashville_housing_data; -- Trims off last word in a string

ALTER TABLE nashville_housing_data
ADD PropertyAddressSplit VARCHAR(255);

UPDATE nashville_housing_data
SET PropertyAddressSplit = RTRIM(REVERSE(SUBSTRING(REVERSE(PropertyAddress), LOCATE(' ', REVERSE(PropertyAddress)))));

ALTER TABLE nashville_housing_data
ADD PropertyCitySplit VARCHAR(255);

UPDATE nashville_housing_data
SET PropertyCitySplit = REVERSE(SUBSTRING(REVERSE(PropertyAddress), 1, LOCATE(' ', REVERSE(PropertyAddress)) - 1));

SELECT OwnerAddress
FROM nashville_housing_data;

SELECT
  SUBSTRING_INDEX(OwnerAddress, ' ', -1) AS State,
  SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ' ', -2), ' ', 1) AS City,
  TRIM(TRAILING SUBSTRING_INDEX(OwnerAddress, ' ', -2) FROM OwnerAddress) AS Address
FROM
  nashville_housing_data;
  
ALTER TABLE nashville_housing_data
ADD OwnerSplitState VARCHAR(255);

UPDATE nashville_housing_data
SET OwnerSplitState = SUBSTRING_INDEX(OwnerAddress, ' ', -1);

ALTER TABLE nashville_housing_data
ADD OwnerSplitCity VARCHAR(255);

UPDATE nashville_housing_data
SET OwnerSplitCity = SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ' ', -2), ' ', 1);

ALTER TABLE nashville_housing_data
ADD OwnerSplitAddress VARCHAR(255);

UPDATE nashville_housing_data
SET OwnerSplitAddress = TRIM(TRAILING SUBSTRING_INDEX(OwnerAddress, ' ', -2) FROM OwnerAddress);

SELECT OwnerSplitAddress, OwnerSplitCity, OwnerSplitState
FROM nashville_housing_data;

-- ----------------------------------------------------------------------------------------------------

-- Change Y and N to Yes and No in 'Sold as Vacant' field

SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM nashville_housing_data
GROUP BY SoldAsVacant
ORDER BY 2;

SELECT SoldAsVacant
, CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
	   WHEN SoldAsVacant = 'N' THEN 'No'
       ELSE SoldAsVacant
       END
FROM nashville_housing_data;

UPDATE nashville_housing_data
SET SoldAsVacant = CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
						WHEN SoldAsVacant = 'N' THEN 'No'
						ELSE SoldAsVacant
						END
;

-- ----------------------------------------------------------------------------------------------------

-- Remove duplicates

SELECT *
FROM 
(
	SELECT 
    *, 	
    ROW_NUMBER() OVER (
		PARTITION BY ParcelID,
				 PropertyAddress,
                 SalePrice,
                 SaleDate,
                 LegalReference
                 ORDER BY
					UniqueID
				 ) row_num
		FROM nashville_housing_data
) t
WHERE row_num > 1;

DELETE FROM nashville_housing_data
WHERE UniqueID IN (
SELECT UniqueID
FROM 
(
	SELECT 
    UniqueID, 	
    ROW_NUMBER() OVER (
		PARTITION BY ParcelID,
				 PropertyAddress,
                 SalePrice,
                 SaleDate,
                 LegalReference
                 ORDER BY
					UniqueID
				 ) row_num
		FROM nashville_housing_data
) t
WHERE row_num > 1
);

-- ----------------------------------------------------------------------------------------------------

-- Delete unused columns

SELECT *
FROM nashville_housing_data;

ALTER TABLE nashville_housing_data 
DROP COLUMN PropertyAddress;

ALTER TABLE nashville_housing_data 
DROP COLUMN OwnerAddress;
