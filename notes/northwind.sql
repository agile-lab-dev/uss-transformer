PRIMARY KEY categories(category_id);
PRIMARY KEY customer_customer_demo(customer_id, customer_type_id);
PRIMARY KEY customer_demographics(customer_type_id);
PRIMARY KEY customers(customer_id);
PRIMARY KEY employee_territories(employee_id, territory_id);
PRIMARY KEY employees(employee_id);
PRIMARY KEY order_details(order_id, product_id);
PRIMARY KEY orders(order_id);
PRIMARY KEY products(product_id);
PRIMARY KEY region(region_id);
PRIMARY KEY shippers(shipper_id);
PRIMARY KEY suppliers(supplier_id);
PRIMARY KEY territories(territory_id);
PRIMARY KEY us_states(state_id);
FOREIGN KEY customer_customer_demo(customer_type_id) REFERENCES customer_demographics(customer_type_id);
FOREIGN KEY customer_customer_demo(customer_id) REFERENCES customers(customer_id);
FOREIGN KEY employee_territories(employee_id) REFERENCES employees(employee_id);
FOREIGN KEY employee_territories(territory_id) REFERENCES territories(territory_id);
FOREIGN KEY employees(reports_to) REFERENCES employees(employee_id);
FOREIGN KEY order_details(order_id) REFERENCES orders(order_id);
FOREIGN KEY order_details(product_id) REFERENCES products(product_id);
FOREIGN KEY orders(customer_id) REFERENCES customers(customer_id);
FOREIGN KEY orders(employee_id) REFERENCES employees(employee_id);
FOREIGN KEY orders(ship_via) REFERENCES shippers(shipper_id);
FOREIGN KEY products(category_id) REFERENCES categories(category_id);
FOREIGN KEY products(supplier_id) REFERENCES suppliers(supplier_id);
FOREIGN KEY territories(region_id) REFERENCES region(region_id);