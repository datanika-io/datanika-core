CREATE TABLE goods_ratings (
    id INT IDENTITY(1,1) PRIMARY KEY,
    good_id INT NOT NULL,
    user_id INT NOT NULL,
    rating TINYINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    created_at DATETIME2 NOT NULL
);

CREATE INDEX idx_goods_ratings_good_id ON goods_ratings(good_id);
CREATE INDEX idx_goods_ratings_user_id ON goods_ratings(user_id);

CREATE TABLE reviews (
    id INT IDENTITY(1,1) PRIMARY KEY,
    good_id INT NOT NULL,
    user_id INT NOT NULL,
    title NVARCHAR(300) NOT NULL,
    body NVARCHAR(MAX) NOT NULL,
    created_at DATETIME2 NOT NULL
);

CREATE INDEX idx_reviews_good_id ON reviews(good_id);
CREATE INDEX idx_reviews_user_id ON reviews(user_id);
