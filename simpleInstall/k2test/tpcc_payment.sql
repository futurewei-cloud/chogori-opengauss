BEGIN;
SELECT w_ytd FROM warehouse WHERE w_id = 1; 
UPDATE warehouse SET w_ytd = 250.5 WHERE w_id = 1;
SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = 1;
SELECT d_ytd FROM district WHERE d_w_id = 1 ANd d_id = 1;
UPDATE district SET d_ytd = 300.0 WHERE d_w_id = 1 AND d_id = 1;
SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = 1 and d_id = 1; 
SELECT c_first, c_middle, c_last, c_street_1, c_street_2, \
    c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, \
    c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer \
    WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
SELECT c_data FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = 50.0, c_ytd_payment = 20.5, c_payment_cnt = 5, c_data = 'new data' \
    WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = 12.5, c_ytd_payment = 98.5, c_payment_cnt = 3 \
    WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) \
    VALUES (2, 2, 2, 3, 4, '1999-01-08 04:05:06', 20.0, 'history data');
SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, \
    c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, \
    c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer \
    WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'a' ORDER BY c_first;
COMMIT;