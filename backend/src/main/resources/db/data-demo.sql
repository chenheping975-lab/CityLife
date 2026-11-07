-- Synthetic demo data only. Run after schema.sql in a new demo database.
-- No users, phone numbers, passwords, orders, or real business records are included.
SET NAMES utf8mb4;

INSERT INTO tb_shop_type (id, name, icon, sort) VALUES
    (1, '美食', '/types/ms.png', 1),
    (2, 'KTV', '/types/KTV.png', 2),
    (3, '丽人·美发', '/types/lrmf.png', 3),
    (4, '健身运动', '/types/jsyd.png', 4),
    (5, '按摩·足疗', '/types/amzl.png', 5),
    (6, '美容SPA', '/types/spa.png', 6),
    (7, '亲子游乐', '/types/qzyl.png', 7),
    (8, '酒吧', '/types/jiuba.png', 8),
    (9, '轰趴馆', '/types/hpg.png', 9),
    (10, '美睫·美甲', '/types/mjmj.png', 10);

INSERT INTO tb_shop
    (id, name, type_id, images, area, address, x, y, avg_price, sold, comments, score, open_hours)
VALUES
    (1, 'CityLife 演示咖啡馆', 1, '/imgs/blogs/blog1.jpg', '演示商圈',
     '虚构地址，仅供本地演示', 120.15, 30.28, 30, 0, 0, 45, '09:00-21:00');
