PRIMARY KEY employees(employee_id);
PRIMARY KEY customers(customer_id);
FOREIGN KEY phonecalls(employee_id) REFERENCES employees(employee_id);
FOREIGN KEY phonecalls(customer_id) REFERENCES customers(customer_id);
FOREIGN KEY emails(employee_id) REFERENCES employees(employee_id);
FOREIGN KEY emails(customer_id) REFERENCES customers(customer_id);
FOREIGN KEY deals(employee_id) REFERENCES employees(employee_id);
FOREIGN KEY deals(customer_id) REFERENCES customers(customer_id);