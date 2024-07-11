-- verify total movies
SELECT 
	COUNT(DISTINCT imdb_code) as total_movie 
FROM fact_movie as fm
INNER JOIN dim_genre dg
ON fm.genre_id = dg.genre_id
WHERE dg.genre = 'Thriller' 

-- average rating 
SELECT 
	avg(fm.rating) as average_rating 
FROM fact_movie as fm
INNER JOIN dim_genre dg
ON fm.genre_id = dg.genre_id
WHERE dg.genre = 'Thriller' 

-- average movie length 
SELECT 
	avg(fm.runtime) as average_movie_length 
FROM fact_movie as fm
INNER JOIN dim_genre dg
ON fm.genre_id = dg.genre_id
WHERE dg.genre = 'Thriller' 

-- total vote count
SELECT 
	sum(fm.votecount) as total_vote_count 
FROM fact_movie as fm
INNER JOIN dim_genre dg
ON fm.genre_id = dg.genre_id
WHERE dg.genre = 'Thriller' 


-- recommendatino by genre 
SELECT top 5
	dd.title, 
	avg(rating) as avg_rating
FROM fact_movie as fm
INNER JOIN dim_genre dg
	ON fm.genre_id = dg.genre_id
INNER JOIN dim_details dd
	ON fm.imdb_code = dd.imdb_code
WHERE dg.genre = 'Thriller' 
group by dd.title
order by 2 desc


-- recommendatino by Short or long 
SELECT top 5
	dd.title, 
	avg(rating) as avg_rating
FROM fact_movie as fm
INNER JOIN dim_genre dg
	ON fm.genre_id = dg.genre_id
INNER JOIN dim_details dd
	ON fm.imdb_code = dd.imdb_code
WHERE fm.runtime <= 100
group by dd.title
order by 2 desc


-- another insight page

-- 1. top 3 director by votecount 

SELECT top 3
	dg.director, 
	ROUND(sum(votecount) / 1000000, 2) as total_vote_count
FROM fact_movie as fm
INNER JOIN dim_director dg
	ON fm.director_id = dg.director_id
group by director
order by 2 desc


-- 3. total movie over every year
SELECT
	fm.release_year,
	count(distinct imdb_code) as total_movie
FROM fact_movie as fm
INNER JOIN dim_director dg
	ON fm.director_id = dg.director_id
group by release_year
order by fm.release_year


select * from fact_movie 
inner join dim_details 
on fact_movie.imdb_code = dim_details.imdb_code
where release_year = 2025


---- scatter plot
SELECT 
    dim_genre.genre,  
    ROUND(AVG(fact_movie.worldwideGross_amount) / 1000000, 2) AS average_worldwideGross_million,
    ROUND(AVG(fact_movie.productionBudget_amount) / 1000000, 2) AS average_productionBudget_million
FROM 
    fact_movie
INNER JOIN
    dim_genre
ON 
    fact_movie.genre_id = dim_genre.genre_id
GROUP BY 
    dim_genre.genre;




---------------------

-- Recommendation by age genre


SELECT top 10
	d.title, avg(f.rating) as avg_rating
FROM fact_movie f 
INNER JOIN dim_genre g
ON f.genre_id = g.genre_id
INNER JOIN dim_details d 
ON d.imdb_code = f.imdb_code
WHERE g.genre IN ('Adventure', 'Action', 'Sport', 'Drama', 'History', 'Biography', 'Family', 'Crime')
GROUP BY d.title 
order by 2 desc