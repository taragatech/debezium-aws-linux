CREATE TABLE `TestDB`.`Orders` (
  `Id` INT NOT NULL AUTO_INCREMENT,
  `ProductName` VARCHAR(150) NULL,
  `OrderDate` DATETIME NULL,
  PRIMARY KEY (`Id`));

DELIMITER $$
CREATE PROCEDURE generate_data()
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < 100 DO
    Insert into `TestDB`.`Orders`(ProductName,OrderDate) values (Concat('aa',ROUND(RAND()*100,2)),CurDate());
    SET i = i + 1;
  END WHILE;
END$$
DELIMITER ;

CALL generate_data();

-- drop table `TestDB`.`Orders`
-- truncate table `TestDB`.`Orders`
-- drop procedure generate_data


select * from `TestDB`.`Orders`