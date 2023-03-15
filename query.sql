WITH cte_Top as
(
SELECT Distinct Element = cowUNID_Mother
       ,Parent = NULL
FROM test.test_input
Where
cowUNID_Mother NOT IN (SELECT cowUNID FROM test.test_input )
UNION ALL
SELECT DISTINCT
       cowUNID
       ,cowUNID_Mother
FROM  test.test_input
),
cte AS
(
  SELECT
        Element
       ,Parent
       ,1 as level
       ,Element as level1
       ,Element as level2
       ,Element as level3
       ,Element as level4
       ,Element as level5
       ,Element as level6
       ,Element as level7
       ,Element as level8
       ,Element as level9
  FROM cte_Top
  WHERE Parent  IS NULL
  UNION ALL
  SELECT
       i.Element
       ,i.Parent
       ,level+1 as level
       ,c.level1 as level1
       ,case when c.level+1=2 then i.Element else c.level2 end as level2
       ,case when c.level+1<=3 then i.Element else c.level3 end as level3
       ,case when c.level+1<=4 then i.Element else c.level4 end as level4
       ,case when c.level+1<=5 then i.Element else c.level5 end as level5
       ,case when c.level+1<=6 then i.Element else c.level6 end as level6
       ,case when c.level+1<=7 then i.Element else c.level7 end as level7
       ,case when c.level+1<=8 then i.Element else c.level8 end as level8
       ,case when c.level+1<=9 then i.Element else c.level9 end as level9
  FROM cte_Top i
  INNER JOIN cte c
    ON c.Element = i.Parent
)
SELECT T.* FROM (SELECT Element AS cowUNID ,
          c.level AS LevelDepth,
          Parent,
          CHILD = IIF(c.level >= 2,Level2,NULL),
          GRAND_CHILD = IIF(c.level >= 3,Level3,NULL),
          GREAT_GRAND_CHILD = IIF(c.level >= 4,Level4,NULL)
FROM cte c) T  WHERE CHILD is not NULL