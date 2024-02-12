-- Create the employee table in DB2
CREATE TABLE employees( 
emp_id SERIAL,
first_name VARCHAR(100), 
last_name VARCHAR(100), 
dob DATE,
city VARCHAR(100) );

-- Test the synchronized changes in employee table 
select * from employees e 
