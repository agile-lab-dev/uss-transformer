--
-- PostgreSQL database dump
--

-- Dumped from database version 14.13 (Ubuntu 14.13-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.13 (Ubuntu 14.13-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: loops; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA loops;


ALTER SCHEMA loops OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: customers; Type: TABLE; Schema: loops; Owner: postgres
--

CREATE TABLE loops.customers (
    customer_id character(1) NOT NULL
);


ALTER TABLE loops.customers OWNER TO postgres;

--
-- Name: deals; Type: TABLE; Schema: loops; Owner: postgres
--

CREATE TABLE loops.deals (
    employee_id character(4),
    customer_id character(1),
    signed_date date,
    valid_from_date date,
    link_to_doc character varying(40)
);


ALTER TABLE loops.deals OWNER TO postgres;

--
-- Name: emails; Type: TABLE; Schema: loops; Owner: postgres
--

CREATE TABLE loops.emails (
    sent_date date,
    employee_id character(4),
    customer_id character(1),
    subject character varying(20),
    body character varying(40)
);


ALTER TABLE loops.emails OWNER TO postgres;

--
-- Name: employees; Type: TABLE; Schema: loops; Owner: postgres
--

CREATE TABLE loops.employees (
    employee_id character(4) NOT NULL
);


ALTER TABLE loops.employees OWNER TO postgres;

--
-- Name: phonecalls; Type: TABLE; Schema: loops; Owner: postgres
--

CREATE TABLE loops.phonecalls (
    call_date date,
    employee_id character(4),
    customer_id character(1),
    call_status character varying(20),
    topic character varying(20),
    long_description character varying(40)
);


ALTER TABLE loops.phonecalls OWNER TO postgres;

--
-- Data for Name: customers; Type: TABLE DATA; Schema: loops; Owner: postgres
--

INSERT INTO loops.customers (customer_id) VALUES ('A');
INSERT INTO loops.customers (customer_id) VALUES ('B');
INSERT INTO loops.customers (customer_id) VALUES ('C');
INSERT INTO loops.customers (customer_id) VALUES ('D');
INSERT INTO loops.customers (customer_id) VALUES ('E');
INSERT INTO loops.customers (customer_id) VALUES ('F');
INSERT INTO loops.customers (customer_id) VALUES ('G');
INSERT INTO loops.customers (customer_id) VALUES ('H');


--
-- Data for Name: deals; Type: TABLE DATA; Schema: loops; Owner: postgres
--

INSERT INTO loops.deals (employee_id, customer_id, signed_date, valid_from_date, link_to_doc) VALUES ('Emp4', 'A', '2000-01-15', '2000-07-01', 'https://docs...');
INSERT INTO loops.deals (employee_id, customer_id, signed_date, valid_from_date, link_to_doc) VALUES ('Emp4', 'F', '2000-01-15', '2000-04-01', 'https://docs...');
INSERT INTO loops.deals (employee_id, customer_id, signed_date, valid_from_date, link_to_doc) VALUES ('Emp4', 'G', '2000-01-15', '2000-07-01', 'https://docs...');


--
-- Data for Name: emails; Type: TABLE DATA; Schema: loops; Owner: postgres
--

INSERT INTO loops.emails (sent_date, employee_id, customer_id, subject, body) VALUES ('2000-01-01', 'Emp1', 'C', 'Email survey', 'Dear Customer, ...');
INSERT INTO loops.emails (sent_date, employee_id, customer_id, subject, body) VALUES ('2000-01-01', 'Emp1', 'D', 'Email survey', 'Dear Customer, ...');
INSERT INTO loops.emails (sent_date, employee_id, customer_id, subject, body) VALUES ('2000-01-01', 'Emp1', 'E', 'Email survey', 'Dear Customer, ...');
INSERT INTO loops.emails (sent_date, employee_id, customer_id, subject, body) VALUES ('2000-01-02', 'Emp3', 'A', 'Incident 1234 solved', 'We have solved...');


--
-- Data for Name: employees; Type: TABLE DATA; Schema: loops; Owner: postgres
--

INSERT INTO loops.employees (employee_id) VALUES ('Emp1');
INSERT INTO loops.employees (employee_id) VALUES ('Emp2');
INSERT INTO loops.employees (employee_id) VALUES ('Emp3');
INSERT INTO loops.employees (employee_id) VALUES ('Emp4');
INSERT INTO loops.employees (employee_id) VALUES ('Emp5');
INSERT INTO loops.employees (employee_id) VALUES ('Emp6');


--
-- Data for Name: phonecalls; Type: TABLE DATA; Schema: loops; Owner: postgres
--

INSERT INTO loops.phonecalls (call_date, employee_id, customer_id, call_status, topic, long_description) VALUES ('2000-01-01', 'Emp1', 'A', 'Busy', 'Sell new insurance', 'Busy');
INSERT INTO loops.phonecalls (call_date, employee_id, customer_id, call_status, topic, long_description) VALUES ('2000-01-02', 'Emp1', 'A', 'Answered', 'Sell new insurance', 'Was not interested');
INSERT INTO loops.phonecalls (call_date, employee_id, customer_id, call_status, topic, long_description) VALUES ('2000-01-02', 'Emp2', 'B', 'No answer', 'Phone survey', 'No answer');
INSERT INTO loops.phonecalls (call_date, employee_id, customer_id, call_status, topic, long_description) VALUES ('2000-01-03', 'Emp2', 'B', 'Answered', 'Phone survey', 'Survey completed');
INSERT INTO loops.phonecalls (call_date, employee_id, customer_id, call_status, topic, long_description) VALUES ('2000-01-03', 'Emp2', 'C', 'Answered', 'Phone survey', 'She declined the survey');


--
-- Name: customers customers_pkey; Type: CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (customer_id);


--
-- Name: employees employees_pkey; Type: CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.employees
    ADD CONSTRAINT employees_pkey PRIMARY KEY (employee_id);


--
-- Name: deals deals_customer_id_fkey; Type: FK CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.deals
    ADD CONSTRAINT deals_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES loops.customers(customer_id);


--
-- Name: deals deals_employee_id_fkey; Type: FK CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.deals
    ADD CONSTRAINT deals_employee_id_fkey FOREIGN KEY (employee_id) REFERENCES loops.employees(employee_id);


--
-- Name: emails emails_customer_id_fkey; Type: FK CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.emails
    ADD CONSTRAINT emails_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES loops.customers(customer_id);


--
-- Name: emails emails_employee_id_fkey; Type: FK CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.emails
    ADD CONSTRAINT emails_employee_id_fkey FOREIGN KEY (employee_id) REFERENCES loops.employees(employee_id);


--
-- Name: phonecalls phonecalls_customer_id_fkey; Type: FK CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.phonecalls
    ADD CONSTRAINT phonecalls_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES loops.customers(customer_id);


--
-- Name: phonecalls phonecalls_employee_id_fkey; Type: FK CONSTRAINT; Schema: loops; Owner: postgres
--

ALTER TABLE ONLY loops.phonecalls
    ADD CONSTRAINT phonecalls_employee_id_fkey FOREIGN KEY (employee_id) REFERENCES loops.employees(employee_id);


--
-- PostgreSQL database dump complete
--

