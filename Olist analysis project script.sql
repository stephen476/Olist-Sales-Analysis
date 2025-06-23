CREATE DATABASE IF NOT EXISTS ecommerce;

USE ecommerce;

CREATE TABLE orders /* creation of first table */
(
order_id VARCHAR(255) NOT NULL,
customer_id VARCHAR(255) NOT NULL,
order_status VARCHAR(255),
order_purchase_timestamp TIMESTAMP,
order_approved_at TIMESTAMP,
order_delivered_carrier_date TIMESTAMP,
order_delivered_customer_date TIMESTAMP,
order_estimated_delivery_date DATE,
PRIMARY KEY (order_id),
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
); -- end of creation of first table

SELECT * FROM orders;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_orders_dataset.csv' INTO TABLE orders
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into orders table 

UPDATE orders SET order_approved_at = NULL WHERE order_approved_at = 0000-00-00;
UPDATE orders SET order_delivered_carrier_date = NULL WHERE order_delivered_carrier_date = 0000-00-00;
UPDATE orders SET order_delivered_customer_date = NULL WHERE order_delivered_customer_date = 0000-00-00;

SHOW VARIABLES LIKE "local_infile";
/* check for local infile value and enable it */
SET GLOBAL local_infile = 1;

-- --------------------------------------------------------------------------------------------------

CREATE TABLE product_category_name /* creation of second table */
(
product_category_name VARCHAR(255),
product_category_name_english VARCHAR(255)
); -- end of creation of second table

SELECT * FROM product_category_name;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/product_category_name_translation.csv' INTO TABLE product_category_name
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into product_category_name table

-- --------------------------------------------------------------------------------------------------
  
CREATE TABLE sellers /* creation of third table */
(
seller_id VARCHAR(255) NOT NULL,
seller_zip_code_prefix INT,
seller_city VARCHAR(255),
seller_state VARCHAR(255),
PRIMARY KEY (seller_id)
); -- end of creation of third table
  
select * FROM sellers;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_sellers_dataset.csv' INTO TABLE sellers
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel into sellers table

-- --------------------------------------------------------------------------------------------------

CREATE TABLE products /* creation of fourth table */
(
product_id VARCHAR(255),
product_category_name VARCHAR(255),
product_description_lenght INT,
product_photos_quantity INT,
product_weight_g INT,
product_length_cm INT,
product_height_cm INT,
product_width_cm INT
); -- end of creation of fourth table 

SELECT * FROM products;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_products_dataset.csv' INTO TABLE products
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data excel from file into products table

UPDATE products SET product_category_name = NULL WHERE product_category_name = '' OR 0;
UPDATE products SET product_description_lenght = NULL WHERE product_description_lenght = '' OR 0;
UPDATE products SET product_photos_quantity = NULL WHERE product_photos_quantity = '' OR 0;
UPDATE products SET product_weight_g = NULL WHERE product_weight_g = '' OR 0;
UPDATE products SET product_length_cm = NULL WHERE product_length_cm = '' OR 0;
UPDATE products SET product_height_cm = NULL WHERE product_height_cm = '' OR 0;
UPDATE products SET product_width_cm = NULL WHERE product_width_cm = '' OR 0;
UPDATE products SET product_id = TRIM(both '"' FROM product_id);

-- --------------------------------------------------------------------------------------------------

CREATE TABLE order_reviews /* creation of fifth table */
(
review_id VARCHAR(255),
order_id VARCHAR(255),
review_score INT,
review_comment_title VARCHAR(255),
review_comment_message VARCHAR(9999),
review_creation_date date,
review_answer_timestamp TIMESTAMP
); -- end of creation of fifth table

SELECT * FROM order_reviews;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_order_reviews_dataset.csv' INTO TABLE order_reviews
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
ESCAPED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 LINES; -- load data from excel file into order_reviews table

UPDATE order_reviews SET review_comment_title = NULL WHERE review_comment_title = '' OR 0;
UPDATE order_reviews SET review_comment_message = NULL WHERE review_comment_message = '' OR 0;

-- --------------------------------------------------------------------------------------------------

create table order_payments /* creation of sixth table */
(
order_id VARCHAR(255),
payment_sequential INT,
payment_type VARCHAR(255),
payment_installations INT,
payment_value DECIMAL(65,2)
); -- end of creation of sixth table

SELECT * FROM order_payments;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_order_payments_dataset.csv' INTO TABLE order_payments
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into order_payment table

UPDATE order_payments SET payment_value = NULL WHERE payment_value = 0.00;
UPDATE order_payments SET order_id = TRIM(both '"' FROM order_id);

-- --------------------------------------------------------------------------------------------------

CREATE TABLE order_items /* creation of seventh table */
(
order_id VARCHAR(255),
order_item_ount INT,
product_id VARCHAR(255),
seller_id VARCHAR(255),
shipping_limit_date TIMESTAMP,
price DECIMAL(65,2),
freight_value DECIMAL(65,2)
); -- end of creation of seventh table

SELECT * FROM order_items;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_order_items_dataset.csv' INTO TABLE order_items
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into order_items table

UPDATE order_items SET freight_value = NULL WHERE freight_value = 0.00;

-- --------------------------------------------------------------------------------------------------

CREATE TABLE geolocation /* creation of eighth table*/
(
geolocation_zip_code_prefix INT,
geolocation_lat DECIMAL(65,8),
geolocation_lon DECIMAL(65,8),
geolocation_city VARCHAR(255),
geolocation_state VARCHAR(255)
); -- end of creation of eighth table

SELECT * FROM geolocation;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_geolocation_dataset.csv' INTO TABLE geolocation
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel into geolocation table

-- --------------------------------------------------------------------------------------------------

CREATE TABLE customers /* creation of ninth table */
(
customer_id VARCHAR(255) NOT NULL,
customer_unique_id VARCHAR(255) NOT NULL,
customer_zip_code_prefix INT,
customer_city VARCHAR(255),
customer_state VARCHAR(255),
PRIMARY KEY (customer_id)
); -- end of creation of ninth table

SELECT * FROM customers;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_customers_dataset.csv' INTO TABLE customers
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into customers table

-- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT AVG(price_per_month),
       AVG(orders) 
  FROM (
        SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month,
               COUNT(*) AS orders,
               SUM(price) AS price_per_month
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY month
          ORDER BY month
	    ) sub; -- average revenue per month for dataset 826,058.522500 avg orders 6874

SELECT LEFT(order_purchase_timestamp, 4) AS year,
       SUBSTR(order_purchase_timestamp, 6, 2) AS month,
       COUNT(orders.order_id) AS orders
  FROM ecommerce.order_items
  LEFT JOIN ecommerce.products
  ON order_items.product_id = products.product_id
  LEFT JOIN orders
  ON order_items.order_id = orders.order_id
  GROUP BY year, month
  ORDER BY year, month; -- orders per month with year for entire dataset

SELECT (SELECT SUM(total_orders) 
  FROM (SELECT customer_state,
               SUM(price) AS sum_price,
               SUM(freight_value) AS sum_freight_value,
               COUNT(order_id) AS total_orders
		  FROM (
                SELECT order_purchase_timestamp,
                       price, 
                       freight_value,
                       customer_zip_code_prefix,
                       customer_state,
                       orders.order_id 
				  FROM ecommerce.orders
		          LEFT JOIN order_items
                    ON orders.order_id = order_items.order_id
                  LEFT JOIN customers 
                    ON orders.customer_id = customers.customer_id
                ) sub
         GROUP BY customer_state
         ORDER BY total_orders DESC
         LIMIT 2
        ) sub2
        ) /
(SELECT SUM(total_orders) 
   FROM (SELECT customer_state,
                SUM(price) AS sum_price,
                SUM(freight_value) AS sum_freight_value,
                COUNT(order_id) AS total_orders
           FROM (
				 SELECT order_purchase_timestamp,
                        price,
                        freight_value,
                        customer_state,
                        orders.order_id 
                   FROM ecommerce.orders
				   LEFT JOIN order_items
                     ON orders.order_id = order_items.order_id
                   LEFT JOIN customers 
                     ON orders.customer_id = customers.customer_id
                 ) sub
          GROUP BY customer_state
          ORDER BY total_orders DESC
         ) sub2
 )*100 AS percent; -- top 2 states SP and RJ by total sales together produce 55 percent of the orders

SELECT year, 
       ROUND(sales) AS sales, 
       order_count, 
       ROUND(aov) AS aov, 
       COALESCE(ROUND(sales_growth), '') AS sales_growth, 
       COALESCE(ROUND(order_count_growth), '') AS order_count_growth, 
       TRIM('.' FROM LEFT(COALESCE(aov_growth, ''), 4)) AS aov_growth 
  FROM (
        SELECT year,
               sales, 
               order_count, 
               aov, 
               sale_difference/LAG(sales) OVER(ORDER BY year)*100 AS sales_growth,
               order_count_difference/LAG(order_count) OVER(ORDER BY year)*100 AS order_count_growth,
               aov_difference/LAG(aov) OVER(ORDER BY year)*100 AS aov_growth 
          FROM (
                SELECT *
				  FROM (
				        SELECT *, 
					           sales/order_count AS aov 
                          FROM (
                                SELECT LEFT(order_purchase_timestamp, 4) AS year,
								       SUM(price) AS sales,
                                       COUNT(orders.order_id) AS order_count
								  FROM ecommerce.order_items
                                  LEFT JOIN ecommerce.orders
                                    ON order_items.order_id = orders.order_id
                                 GROUP BY year
                                 ORDER BY year 
                                ) sub1
                        ) sub2 
				  LEFT JOIN
(SELECT *, 
		sub4_sale - LAG(sub4_sale) OVER(ORDER BY sub4_year) AS sale_difference,
        sub4_order_count - LAG(sub4_order_count) OVER(ORDER BY sub4_year) AS order_count_difference,
        sub4_aov - LAG(sub4_aov) OVER(order by sub4_year) AS aov_difference
   FROM (
          SELECT *,
                 sub4_sale/sub4_order_count AS sub4_aov 
			FROM (
				  SELECT LEFT(order_purchase_timestamp, 4) AS sub4_year, 
						 SUM(price) AS sub4_sale,
					     COUNT(orders.order_id) AS sub4_order_count
				    FROM ecommerce.order_items
                    LEFT JOIN ecommerce.orders
                      ON order_items.order_id = orders.order_id
				   GROUP BY sub4_year
				   ORDER BY sub4_year 
				  ) sub1
         ) sub2
 ) sub4
ON sub2.year = sub4.sub4_year
                ) sub5
        ) sub6; -- AOV, sales growth, aov growth, order count growth per year

SELECT *, 
       DENSE_RANK() OVER (PARTITION BY customer_state ORDER BY avg_review_score DESC) AS review_rank 
  FROM (
        SELECT year,
               month, 
               customer_state, 
               AVG(review_score) AS avg_review_score
		  FROM (
				SELECT LEFT(order_purchase_timestamp, 4) AS year,
                       SUBSTR(order_purchase_timestamp, 6, 2) AS month,
                       customer_state, 
                       review_score
				  FROM ecommerce.orders
				  LEFT JOIN order_reviews
				    ON orders.order_id = order_reviews.order_id
				  LEFT JOIN customers     
				    ON orders.customer_id = customers.customer_id
				) sub1
		  GROUP BY year, month, customer_state
          ORDER BY year, month, customer_state
        ) sub2 
 ORDER BY year, month, customer_state
; -- avg review per state per month with review score and dense rank

SELECT AVG(review_score) AS avg_score_per_state, 
       customer_state 
  FROM (
		SELECT order_purchase_timestamp, 
               review_score, 
               customer_state
          FROM ecommerce.order_reviews
          LEFT JOIN orders
          ON order_reviews.order_id = orders.order_id
          LEFT JOIN customers
          ON orders.customer_id = customers.customer_id
          LEFT JOIN order_items 
          ON orders.order_id = order_items.order_id
          LEFT JOIN products 
          ON order_items.product_id = products.product_id
        ) sub1
 GROUP BY customer_state
 ORDER BY avg_score_per_state DESC;
-- avg review score per state

SELECT product_category_name_english, 
       total_revenue, 
       ROUND(total_revenue_percent) AS "total_revenue(%)",
       ROUND(AOV) AS AOV, 
       order_count, 
       ROUND(order_count_percent) AS "order_count(%)" 
  FROM (
        SELECT product_category_name_english, 
               total_revenue, 
               total_revenue/13591643.70*100 AS total_revenue_percent,
               total_revenue/order_count AS AOV, 
               order_count, order_count/99441*100 AS order_count_percent
          FROM (
                SELECT product_category_name_english, 
                       SUM(price) AS total_revenue, 
                       COUNT(*) AS order_count
				  FROM ecommerce.order_items
				  LEFT JOIN ecommerce.products
                    ON order_items.product_id = products.product_id
		          LEFT JOIN product_category_name
                    ON products.product_category_name = product_category_name.product_category_name
                 GROUP BY product_category_name_english
				 ORDER BY total_revenue DESC
                 LIMIT 8
                ) sub1
	    ) sub2; -- top 8 categories with total revenue, AOV, order count

SELECT *, 
       CONCAT(ROUND((repeat_customer/unique_customer)*100, 1), '%') AS repeat_rate
  FROM (
		SELECT year, 
               unique_customer, 
               total_customer - unique_customer AS repeat_customer 
		  FROM (
				SELECT year, 
                       COUNT(customer_unique_id) AS total_customer,
                       COUNT(DISTINCT(customer_unique_id)) AS unique_customer  
				  FROM (
						SELECT LEFT(order_purchase_timestamp, 4) AS year,
                               customer_unique_id  
						  FROM ecommerce.orders
						  LEFT JOIN ecommerce.customers
							ON orders.customer_id = customers.customer_id
						) sub1
				 GROUP BY year
                ) sub2
        ) sub2; -- total repeat purchases and unique customers in each year with repeat rate
        
SELECT AVG(sub1_avg_review_score),
       AVG(sub2_avg_review_score), AVG(sub3_avg_review_score) 
  FROM (
        SELECT customer_state AS sub1_customer_state,
               AVG(review_score) AS sub1_avg_review_score
          FROM (
				SELECT *  
				  FROM (
						SELECT orders.order_id,
                               order_status, 
                               order_purchase_timestamp,
                               order_approved_at,
					           order_delivered_carrier_date,
                               LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date,
                               customer_state,
                               order_estimated_delivery_date,
                               review_score
                               FROM ecommerce.orders
                               LEFT JOIN order_reviews
                               ON orders.order_id = order_reviews.order_id
                               LEFT JOIN customers
                               ON orders.customer_id = customers.customer_id
						) sub1
				 WHERE order_delivered_customer_date > order_estimated_delivery_date
				 ORDER BY review_score DESC
                ) sub2
          GROUP BY sub1_customer_state
          ORDER BY sub1_avg_review_score DESC
		) sub1
LEFT JOIN (
           SELECT customer_state AS sub2_customer_state,
                  AVG(review_score) AS sub2_avg_review_score
			 FROM (
				   SELECT *  
					 FROM (
						   SELECT orders.order_id,
                                  order_status,
								  order_purchase_timestamp, 
                                  order_approved_at,
                                  order_delivered_carrier_date,
                                  LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date,
                                  customer_state,
                                  order_estimated_delivery_date,
                                  review_score
                                  FROM ecommerce.orders
							      LEFT JOIN order_reviews
                                  ON orders.order_id = order_reviews.order_id
                                  LEFT JOIN customers
                                  ON orders.customer_id = customers.customer_id
                           ) sub1
					WHERE order_delivered_customer_date < order_estimated_delivery_date
					ORDER BY review_score DESC
                   ) sub2
            GROUP BY sub2_customer_state
            ORDER BY sub2_avg_review_score DESC
           ) sub2
ON sub1.sub1_customer_state = sub2.sub2_customer_state
LEFT JOIN (
           SELECT customer_state AS sub3_customer_state,
                  AVG(review_score) AS sub3_avg_review_score
			 FROM (
				   SELECT *  
					 FROM (
						   SELECT orders.order_id,
                                  order_status, order_purchase_timestamp,
                                  order_approved_at,
								  order_delivered_carrier_date,
						          LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date,
                                  customer_state,    
						          order_estimated_delivery_date,
							      review_score
							 FROM ecommerce.orders
							 LEFT JOIN order_reviews
							 ON orders.order_id = order_reviews.order_id
							 LEFT JOIN customers
							 ON orders.customer_id = customers.customer_id
                           ) sub1
					WHERE order_delivered_customer_date = order_estimated_delivery_date
					ORDER BY review_score DESC
                   ) sub1
            GROUP BY sub3_customer_state
            ORDER BY sub3_avg_review_score DESC
           ) sub3
ON sub2.sub2_customer_state = sub3.sub3_customer_state;
/* average review score when dilivery time is greater than estimated dilivery time is 2.2
average review score when dilivery time is less than estimated dilivery time 4.2 
average review score when dilivery time is equal to estimated dilivery time 4.0*/

SELECT customer_state,
       COUNT(orders.order_id) AS total_orders, 
       SUM(price) AS sum_price
  FROM ecommerce.orders
  LEFT JOIN order_items
    ON orders.order_id = order_items.order_id
  LEFT JOIN customers 
    ON orders.customer_id = customers.customer_id
 GROUP BY customer_state; -- total orders and sum price per state
 
WITH monthly_orders AS (SELECT LEFT(order_purchase_timestamp, 4) AS Year,
			       SUBSTR(order_purchase_timestamp, 6, 2) AS Month,
                   	       COUNT(orders.order_id) AS orders
                          FROM ecommerce.order_items
                          LEFT JOIN ecommerce.products
                            ON order_items.product_id = products.product_id
			  LEFT JOIN orders
                            ON order_items.order_id = orders.order_id
		         GROUP BY Year, Month
                         ORDER BY Year, Month),
   order_difference AS (SELECT Year,
                               Month, 
                               orders,
                               orders - LAG(orders) OVER(ORDER BY year) AS order_growth 
                          FROM monthly_orders)

SELECT Year, Month, order_growth/LAG(orders) OVER(ORDER BY year)*100 AS Order_Growth FROM order_difference; -- monthly orders by year

WITH monthly_sales AS (SELECT LEFT(order_purchase_timestamp, 4) AS Year,
			      SUBSTR(order_purchase_timestamp, 6, 2) AS Month,
			      SUM(price) AS sum_price
			 FROM ecommerce.order_items
			 LEFT JOIN ecommerce.products
			   ON order_items.product_id = products.product_id
			 LEFT JOIN orders
			   ON order_items.order_id = orders.order_id
		        GROUP BY Year, Month
		        ORDER BY Year, Month),
  sales_difference AS (SELECT Year,
                              Month, 
                              sum_price,
                              sum_price - LAG(sum_price) OVER(ORDER BY year) AS sales_growth 
                         FROM monthly_sales)

SELECT Year, Month, sales_growth/LAG(sum_price) OVER(ORDER BY year)*100 AS Sales_Growth FROM sales_difference; -- monthly sales by year

WITH monthly_orders_and_sales AS (SELECT LEFT(order_purchase_timestamp, 4) AS Year,
				         SUBSTR(order_purchase_timestamp, 6, 2) AS Month,
				         COUNT(orders.order_id) AS orders,
                                         SUM(price) AS sum_price
                                    FROM ecommerce.order_items
                                    LEFT JOIN ecommerce.products
                                      ON order_items.product_id = products.product_id
				    LEFT JOIN orders
                                      ON order_items.order_id = orders.order_id
			           GROUP BY Year, Month
                                   ORDER BY Year, Month),
   monthly_aov AS (SELECT Year,
			  Month,
	                  sum_price/orders AS aov 
		     FROM monthly_orders_and_sales),
aov_difference AS (SELECT year,
                          month, 
                          aov, 
                          aov - LAG(aov) OVER(ORDER BY year) AS aov_diff FROM monthly_aov)

SELECT Year, Month, aov_diff/LAG(aov) OVER(ORDER BY year)*100 AS Aov_Growth FROM aov_difference; -- monthly aov by year
