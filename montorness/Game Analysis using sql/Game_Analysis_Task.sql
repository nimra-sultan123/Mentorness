create database game_analysis;

use game_analysis;

-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.
-----------------------------------------------------------------------------------------

select * from player_details
select * from level_details2 


-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0

select pd.P_ID , ld.Dev_ID , pd.PName , ld.Difficulty as Difficulty_level from player_details pd
join level_details2 ld on pd.P_ID = ld.P_ID
where ld.Level = 0;

-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast
-- 3 stages are crossed

select pd.L1_Code,avg(ld.Kill_Count) as Avg_Kill_Count  from player_details pd
join level_details2 ld on pd.P_ID = ld.P_ID
where ld.Lives_Earned = 2 and ld.Stages_crossed >= 3
group by pd.L1_Code

-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result
-- in decsreasing order of total number of stages crossed.

select ld.Difficulty , sum(ld.stages_crossed) as Total_Stages_Crossed from level_details2 ld 
where ld.Level = 2 and ld.Dev_ID  LIKE 'zm_%'
group by ld.Difficulty
order by Total_Stages_Crossed desc

-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.

Select P_ID, COUNT(DISTINCT CONVERT(DATE,ld.TimeStamp )) as total_unique_dates
from level_details2 ld
group by P_ID
having count(DISTINCT CONVERT(DATE, ld.TimeStamp)) > 1;

-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.

WITH MediumAvgKill AS (
    SELECT AVG(kill_count) AS avg_kill_count
    FROM level_details2
    WHERE difficulty = 'Medium'
)
SELECT pd.P_ID, ld.level, SUM(ld.kill_count) AS total_kill_count
FROM player_details pd
JOIN level_details2 ld ON pd.P_ID = ld.P_ID
JOIN MediumAvgKill as avg_kill_table ON ld.difficulty = 'Medium' AND ld.kill_count > avg_kill_table.avg_kill_count
GROUP BY pd.P_ID, ld.level;

-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.

select ld.Level, sum(ld.Lives_Earned) as Total_Lives_Earned from level_details2 ld 
where ld.Level <> 0
group by ld.Level
order by ld.Level asc

-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 

WITH RankedScores AS (
    SELECT 
        Dev_ID,
        Difficulty,
        Score,
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Score ASC) AS ScoreRank
    FROM level_details2
)
SELECT 
    Dev_ID,
    Difficulty,
    Score
FROM RankedScores
WHERE ScoreRank <= 3;

--Q8) Find first_login datetime for each device id

SELECT 
    Dev_ID,
    MIN(CONVERT(varchar, timestamp, 108)) AS first_login_datetime
FROM level_details2
GROUP BY Dev_ID;

-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.

WITH RankedScores AS (
    SELECT
        Dev_ID,
        Difficulty,
        Score,
        RANK() OVER (PARTITION BY Difficulty ORDER BY Score ASC) AS score_rank
    FROM
        level_details2
)
SELECT
    Dev_ID,
    Difficulty,
    Score
FROM
    RankedScores
WHERE
    score_rank <= 5;


-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and 
-- first login datetime.

WITH FirstLogin AS (
    SELECT
        P_ID,
        Dev_ID,
        CONVERT(varchar, timestamp, 108) as Start_Time,
        ROW_NUMBER() OVER (PARTITION BY P_ID ORDER BY CONVERT(varchar, timestamp, 108)) AS login_rank
    FROM
        level_details2
)
SELECT
    P_ID,
    Dev_ID,
    Start_Time AS first_login_datetime
FROM
    FirstLogin
WHERE
    login_rank = 1;

-- Q11) For each player and date, how many kill_count played so far by the player. 
--That is, the total number of games played -- by the player until that date.
-- a) window function
-- b) without window function

SELECT
    P_ID,
    CONVERT(date, TimeStamp) AS play_date,
    SUM(kill_count) OVER (PARTITION BY P_ID ORDER BY CONVERT(date, TimeStamp)) AS total_kill_count
FROM
    level_details2;

-------------------------------------------------------------------------------
SELECT
    ld1.P_ID,
    CONVERT(date, ld1.TimeStamp) AS play_date,
    SUM(ld2.kill_count) AS total_kill_count
FROM
    level_details2 ld1
JOIN
    level_details2 ld2 ON ld1.P_ID = ld2.P_ID
                      AND CONVERT(date, ld1.TimeStamp) >= CONVERT(date, ld2.TimeStamp)
GROUP BY
    ld1.P_ID, CONVERT(date, ld1.TimeStamp);

-- Q12) Find the cumulative sum of stages crossed over a start_datetime 

SELECT
    TimeStamp,
    stages_crossed,
    SUM(stages_crossed) OVER (ORDER BY Timestamp) AS cumulative_stages_crossed
FROM
    level_details2;

-- Q13) Find the cumulative sum of an stages crossed over a start_datetime 
-- for each player id but exclude the most recent start_datetime

WITH RankedData AS (
    SELECT
        P_ID,
        TimeStamp,
        stages_crossed,
        ROW_NUMBER() OVER (PARTITION BY P_ID ORDER BY TimeStamp DESC) AS rn
    FROM
        level_details2
)
SELECT
    P_ID,
    TimeStamp,
    stages_crossed,
    SUM(stages_crossed) OVER (PARTITION BY P_ID ORDER BY Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cumulative_stages_crossed
FROM
    RankedData
WHERE
    rn > 1;

-- Q14) Extract top 3 highest sum of score for each device id and the corresponding player_id

WITH RankedScores AS (
    SELECT
        P_ID,
        Dev_ID,
        SUM(score) AS total_score,
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY SUM(score) DESC) AS score_rank
    FROM
        level_details2
    GROUP BY
        P_ID, Dev_ID
)
SELECT
    P_ID,
    Dev_ID,
    total_score
FROM
    RankedScores
WHERE
    score_rank <= 3;


-- Q15) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id

SELECT
    ld.P_ID,
    SUM(ld.score) AS total_score
FROM
    level_details2 ld
GROUP BY
    ld.P_ID
HAVING
    SUM(ld.score) > 0.5 * (SELECT AVG(total_score) 
FROM (SELECT P_ID, SUM(score) AS total_score FROM level_details2 GROUP BY P_ID) AS avg_scores);

-- Q16) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.

CREATE PROCEDURE GetTopNHeadshotsCount
    @TopN INT
AS
BEGIN
    WITH RankedHeadshots AS (
        SELECT
            Dev_ID,
            Difficulty,
            Headshots_Count,
            ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Headshots_Count) AS Rank
        FROM
            level_details2
    )
    SELECT
        Dev_ID,
        Difficulty,
        Headshots_Count,
        Rank
    FROM
        RankedHeadshots
    WHERE
        Rank <= @TopN
    ORDER BY
        Dev_ID, Rank;
END;

EXEC GetTopNHeadshotsCount @TopN = 3;

-- Q17) Create a function to return sum of Score for a given player_id.

CREATE FUNCTION dbo.GetTotalScoreForPlayer
(
    @PlayerID INT
)
RETURNS INT
AS
BEGIN
    DECLARE @TotalScore INT;

    SELECT @TotalScore = SUM(Score)
    FROM level_details2
    WHERE P_ID = @PlayerID;

    RETURN ISNULL(@TotalScore, 0);
END;

DECLARE @PlayerID INT = 211; 
SELECT dbo.GetTotalScoreForPlayer(@PlayerID) AS TotalScore;

