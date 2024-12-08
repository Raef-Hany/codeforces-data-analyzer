-- Check count of records in each table
SELECT 'Countries' as Table_Name, COUNT(*) as Record_Count FROM Country
UNION ALL
SELECT 'Organizations', COUNT(*) FROM Organization
UNION ALL
SELECT 'Users', COUNT(*) FROM User
UNION ALL
SELECT 'Contests', COUNT(*) FROM Contest
UNION ALL
SELECT 'Problems', COUNT(*) FROM Problem
UNION ALL
SELECT 'Tags', COUNT(*) FROM Tag
UNION ALL
SELECT 'ProblemTags', COUNT(*) FROM ProblemTag;

-- Sample data from main tables
SELECT 'USER TABLE SAMPLE' as Query;
SELECT screen_name, city, contribution, rating, problems_solved 
FROM User 
LIMIT 5;

SELECT 'CONTEST TABLE SAMPLE' as Query;
SELECT name, date, division, participants 
FROM Contest 
LIMIT 5;

SELECT 'PROBLEM TABLE SAMPLE' as Query;
SELECT p.name, p.time_limit, p.memory_limit, 
       GROUP_CONCAT(t.name) as tags
FROM Problem p
LEFT JOIN ProblemTag pt ON p.problem_id = pt.problem_id
LEFT JOIN Tag t ON pt.tag_id = t.tag_id
GROUP BY p.problem_id
LIMIT 5;

-- Check for data integrity
SELECT 'ORPHANED RECORDS CHECK' as Query;

-- Check for users without countries
SELECT COUNT(*) as users_without_country
FROM User 
WHERE country_id IS NULL;

-- Check for problems without tags
SELECT COUNT(*) as problems_without_tags
FROM Problem p
LEFT JOIN ProblemTag pt ON p.problem_id = pt.problem_id
WHERE pt.tag_id IS NULL;

-- Check user statistics
SELECT 'USER STATISTICS' as Query;
SELECT 
    COUNT(*) as total_users,
    AVG(rating) as avg_rating,
    MAX(rating) as max_rating,
    AVG(problems_solved) as avg_problems_solved
FROM User;

-- Check contest statistics
SELECT 'CONTEST STATISTICS' as Query;
SELECT 
    COUNT(*) as total_contests,
    COUNT(DISTINCT division) as number_of_divisions
FROM Contest;

-- Check problem statistics
SELECT 'PROBLEM STATISTICS' as Query;
SELECT 
    COUNT(DISTINCT p.problem_id) as total_problems,
    COUNT(DISTINCT pt.tag_id) as total_unique_tags,
    AVG(time_limit) as avg_time_limit,
    AVG(memory_limit) as avg_memory_limit
FROM Problem p
LEFT JOIN ProblemTag pt ON p.problem_id = pt.problem_id;