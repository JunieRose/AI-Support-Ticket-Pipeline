CREATE TABLE dim_regions(
    region_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_name VARCHAR2(50) NOT NULL
)

INSERT INTO DIM_REGIONS (region_name)
VALUES
    ('CA-TORONTO'),
    ('AU-SYDNEY'),
    ('UK-LONDON'),
    ('SG-CENTRAL'),
    ('JP-TOKYO')


CREATE TABLE dim_categories(
    category_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category_name VARCHAR2(50) NOT NULL
)

INSERT INTO DIM_CATEGORIES (category_name)
VALUES
    ('Technical'),
    ('Billing'),
    ('Account'),
    ('Feedback'),
    ('How-To Question'),
    ('General')

