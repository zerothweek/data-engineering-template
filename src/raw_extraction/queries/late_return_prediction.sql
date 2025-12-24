SELECT 
    r.rental_id,
    r.rental_date,
    r.return_date,
    -- Feature: The limit given to the customer (e.g., 3 days)
    f.rental_duration AS allowed_days,
    -- Feature: Calculate how long they actually kept it
    DATEDIFF(r.return_date, r.rental_date) AS actual_days,
    -- TARGET VARIABLE: 1 if late, 0 if on time
    CASE 
        WHEN DATEDIFF(r.return_date, r.rental_date) > f.rental_duration THEN 1 
        ELSE 0 
    END AS is_late,
    -- Movie Features
    f.title,
    f.length AS film_length_minutes,
    f.rating AS film_rating,
    f.rental_rate,
    f.replacement_cost,
    f.special_features,
    c.name AS category,
    -- Customer/Store Features
    cust.active AS is_active_customer,
    s.store_id
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
JOIN customer cust ON r.customer_id = cust.customer_id
JOIN store s ON i.store_id = s.store_id
-- We only want records where the item has actually been returned to train the model
WHERE r.return_date IS NOT NULL;