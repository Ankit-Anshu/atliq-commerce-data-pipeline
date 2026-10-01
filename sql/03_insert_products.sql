-- ============================================================
--  AtliQ Commerce OLTP  |  Seed data: products
--  Rows: 60
--  Real-world source data: expect inconsistencies (see the guide's Known Issues).
-- ============================================================
SET NOCOUNT ON;

SET IDENTITY_INSERT dbo.products ON;

INSERT INTO dbo.products (product_id, product_name, category, unit_price, updated_at) VALUES
    (1, N'Wireless Earbuds', N'Electronics ', 2499, '2024-12-15 10:00:00'),
    (2, N'Bluetooth Speaker', N'Electronics', 3299, '2024-12-15 10:00:00'),
    (3, N'Power Bank 20000mAh', N'Electronics', 1799, '2024-12-15 10:00:00'),
    (4, N'USB-C Charger 65W', N'Electronics', 1499, '2024-12-15 10:00:00'),
    (5, N'Smartwatch', N'electronics', 4999, '2024-12-15 10:00:00'),
    (6, N'Wireless Mouse', N'Electronics', 899, '2024-12-15 10:00:00'),
    (7, N'Mechanical  Keyboard ', N'Electronics', 3499, '2024-12-15 10:00:00'),
    (8, N'Webcam 1080p', N'Electronics', 2199, '2024-12-15 10:00:00'),
    (9, N'Noise Cancelling Headphones', N'Electronics', 7999, '2024-12-15 10:00:00'),
    (10, N'Portable SSD 1TB', N'Electronics', 6999, '2024-12-15 10:00:00'),
    (11, N'Non-stick Frying Pan', N'Home & Kitchen', 899, '2024-12-15 10:00:00'),
    (12, N'Electric Kettle', N'Home & kitchen', 1299, '2024-12-15 10:00:00'),
    (13, N'Air Fryer', N'Home & Kitchen', 5499, '2024-12-15 10:00:00'),
    (14, N'Mixer Grinder', N'Home & Kitchen', 3299, '2024-12-15 10:00:00'),
    (15, N'Steel Water Bottle', N'Home and Kitchen', 549, '2024-12-15 10:00:00'),
    (16, N'Cotton Bedsheet Set', N'Home & Kitchen', 1199, '2024-12-15 10:00:00'),
    (17, N'LED Desk Lamp', N'Home & Kitchen', 999, '2024-12-15 10:00:00'),
    (18, N'Storage Container Set', N' Home & Kitchen', 749, '2024-12-15 10:00:00'),
    (19, N'Pressure Cooker 5L', N'Home & Kitchen', 2299, '2024-12-15 10:00:00'),
    (20, N'Wall  Clock ', N' Home & Kitchen', 699, '2024-12-15 10:00:00'),
    (21, N'Cotton  T-Shirt ', N'Fashion', 599, '2024-12-15 10:00:00'),
    (22, N'Denim  Jeans ', N'Fashion', 1599, '2024-12-15 10:00:00'),
    (23, N'Running Shoes', N'Fashion', 2999, '2024-12-15 10:00:00'),
    (24, N'Leather Wallet', N'Fashion', 799, '2024-12-15 10:00:00'),
    (25, N'Sunglasses', N'Fashion', 1299, '2024-12-15 10:00:00'),
    (26, N'Casual Sneakers', N'Fashion', 2499, '2024-12-15 10:00:00'),
    (27, N'Winter Jacket', N'Fashion', 3499, '2024-12-15 10:00:00'),
    (28, N'Silk Saree', N'Fashion', 4499, '2024-12-15 10:00:00'),
    (29, N'Cotton Kurta', N'FASHION', 1099, '2024-12-15 10:00:00'),
    (30, N'Canvas Backpack', N'FASHION', 1399, '2024-12-15 10:00:00'),
    (31, N'Data Engineering Handbook', N'Books', 1899, '2024-12-15 10:00:00'),
    (32, N'Python Crash Course', N'Books', 799, '2024-12-15 10:00:00'),
    (33, N'Atomic Habits', N'Books', 499, '2024-12-15 10:00:00'),
    (34, N'SQL  for Analysts ', N'Books', 649, '2024-12-15 10:00:00'),
    (35, N'The Psychology of Money', N'Books', 399, '2024-12-15 10:00:00'),
    (36, N'Sapiens', N'Books', 549, '2024-12-15 10:00:00'),
    (37, N'Clean Code', N'Books', 1299, '2024-12-15 10:00:00'),
    (38, N'Deep Work', N'BOOKS', 449, '2024-12-15 10:00:00'),
    (39, N'The Alchemist', N'Books', 299, '2024-12-15 10:00:00'),
    (40, N'Rich Dad Poor Dad', N'books', 349, '2024-12-15 10:00:00'),
    (41, N'Face Wash', N'Beauty', 249, '2024-12-15 10:00:00'),
    (42, N'Sunscreen SPF 50', N'Beauty', 449, '2024-12-15 10:00:00'),
    (43, N'Hair Serum', N'Beauty', 599, '2024-12-15 10:00:00'),
    (44, N'Moisturiser', N'Beauty', 399, '2024-12-15 10:00:00'),
    (45, N'Lip Balm', N'Beauty', 199, '2024-12-15 10:00:00'),
    (46, N'Beard Oil', N'Beauty', 349, '2024-12-15 10:00:00'),
    (47, N'Vitamin C Serum', N'Beauty', 799, '2024-12-15 10:00:00'),
    (48, N'Shampoo 400ml', N'Beauty', 449, '2024-12-15 10:00:00'),
    (49, N'Perfume 100ml', N'beauty', 1999, '2024-12-15 10:00:00'),
    (50, N'Makeup Brush Set', N'Beauty', 899, '2024-12-15 10:00:00'),
    (51, N'Yoga Mat', N'Sports', 799, '2024-12-15 10:00:00'),
    (52, N'Dumbbell Set 10kg', N'Sports', 1899, '2024-12-15 10:00:00'),
    (53, N'Cricket Bat', N'Sports', 2499, '2024-12-15 10:00:00'),
    (54, N'Football', N'sports', 699, '2024-12-15 10:00:00'),
    (55, N'Skipping Rope', N'Sports', 249, '2024-12-15 10:00:00'),
    (56, N'Badminton Racket', N'Sports', 1299, '2024-12-15 10:00:00'),
    (57, N'Resistance Bands', N'Sports', 599, '2024-12-15 10:00:00'),
    (58, N'Cycling Helmet', N'Sports', 1499, '2024-12-15 10:00:00'),
    (59, N'Gym Gloves', N'SPORTS', 399, '2024-12-15 10:00:00'),
    (60, N'Protein Shaker', N'Sports', 299, '2024-12-15 10:00:00');

SET IDENTITY_INSERT dbo.products OFF;
GO
