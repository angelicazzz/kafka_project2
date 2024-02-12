-- Create the employee table in DB1
CREATE TABLE employees( 
emp_id SERIAL,
first_name VARCHAR(100), 
last_name VARCHAR(100), 
dob DATE,
city VARCHAR(100) );

-- Create the CDC table in DB1
CREATE TABLE employees_cdc( 
emp_id SERIAL,
first_name VARCHAR(100), 
last_name VARCHAR(100), 
dob DATE,
city VARCHAR(100), 
action varchar(100) );

-- Create the trigger function
CREATE OR REPLACE FUNCTION employees_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO employees_cdc (emp_id, first_name, last_name, dob, city, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'INSERT');
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO employees_cdc (emp_id, first_name, last_name, dob, city, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'UPDATE');
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO employees_cdc (emp_id, first_name, last_name, dob, city, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, 'DELETE');
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER employees_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION employees_trigger_function();


-- Test employee table and cdc table
select * from employees e 
select * from employees_cdc ec 

delete from employees e 
delete from employees_cdc ec 

-- Insert test records
insert into employees (emp_id, first_name, last_name, dob, city)
values (1, 'first_A','last_A',null,'New York City')

insert into employees (emp_id, first_name, last_name, dob, city)
values (2, 'first_B','last_B',null,'LA')

insert into employees (emp_id, first_name, last_name, dob, city)
values (3, 'first_C','last_C',null,'SF')

insert into employees (emp_id, first_name, last_name, dob, city)
values (4, 'first_D','last_D',null,'SF')

-- Test case 1: insert record
insert into employees (emp_id, first_name, last_name, dob, city)
values (5, 'first_E','last_E',null,'NYC')

-- Test case 2: delete record based on emp_id
insert into employees (emp_id, first_name, last_name, dob, city)
values (6, 'first_F','last_F',null,'LA')

DELETE FROM employees
WHERE emp_id = 6;

