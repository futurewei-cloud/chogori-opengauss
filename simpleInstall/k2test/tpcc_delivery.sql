BEGIN;
SELECT no_o_id FROM new_order \
    WHERE no_d_id = 1 AND no_w_id = 1 ORDER BY no_o_id ASC LIMIT 1;
DELETE FROM new_order \
    WHERE no_o_id = 5 AND no_d_id = 2 AND no_w_id = 2;
SELECT o_c_id FROM oorder \
    WHERE o_id = 5 AND o_d_id = 1 AND o_w_id = 1;
UPDATE oorder SET o_carrier_id = 2 \
    WHERE o_id = 5 AND o_d_id = 1 AND o_w_id = 1;

UPDATE order_line SET ol_delivery_d = '1999-01-08 04:05:06' \
    WHERE ol_o_id = 5 AND ol_d_id = 1 AND ol_w_id = 1;
SELECT SUM(ol_amount) as ol_total FROM order_line \
    WHERE ol_o_id = 5 AND ol_d_id = 1 AND ol_w_id = 1;
SELECT c_balance, c_delivery_cnt FROM customer \
    WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = 2.0, c_delivery_cnt = 2 \
    WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
COMMIT;