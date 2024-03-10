select columns[0] as name, columns[1] as id, columns[2] as grade from dfs.`/root/grades.csv` order by grade limit 3;

select columns[0] as name, columns[1] as id, avg(cast(columns[2] as int)) as avgScore from dfs.`/root/grades.csv` group by name order by avgScore desc limit 3;

select avg(score) as avgScore from
(
	select row_number() over (order by columns[2]) as ascend, row_number() over (order by columns[2] desc)as descend, columns[0] as name, columns[1] as id, cast (columns[2] as int) as score from dfs.`/root/grades.csv`
)
where descend = ascend or abs(ascend - descend) = 1;
