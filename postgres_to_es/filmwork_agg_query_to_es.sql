SELECT jsonb_agg(json_build_object('id', id,'imdb_rating', imdb_rating,'genre', genre, 'title', title
                                  ,'description', description,'director', director,'actors_names', actors_names
                                  ,'writers_names', writers_names,'actors', actors,'writers', writers)
                                  ) as json
     , max(last_update_at)
FROM (SELECT
    fw.id as id
    ,fw.rating as imdb_rating
    ,json_agg(distinct(g.name)) as genre
    ,fw.title as title
    ,fw.description as description
    ,json_agg(distinct(case when pfw.role = 'director' then p.full_name end))
        filter (WHERE (case when pfw.role = 'director' then p.full_name end) IS NOT NULL) as director
    ,json_agg(distinct(case when pfw.role = 'actor' then p.full_name end))
        filter (WHERE (case when pfw.role = 'actor' then p.full_name end) IS NOT NULL) as actors_names
    ,json_agg(distinct(case when pfw.role = 'writer' then p.full_name end))
        filter (WHERE (case when pfw.role = 'writer' then p.full_name end) IS NOT NULL) as writers_names
    ,json_agg(distinct(case when pfw.role = 'actor' then jsonb_build_object(p.id, p.full_name) end))
        filter (WHERE (case when pfw.role = 'actor' then jsonb_build_object(p.id, p.full_name) end) IS NOT NULL) as actors
    ,json_agg(distinct(case when pfw.role = 'writer' then jsonb_build_object(p.id, p.full_name) end))
        filter (WHERE (case when pfw.role = 'writer' then jsonb_build_object(p.id, p.full_name) end) IS NOT NULL) as writers
    ,greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at)) as last_update_at

    FROM content.film_work as fw
    LEFT JOIN content.genre_film_work gf on gf.film_work_id = fw.id
    LEFT JOIN content.genre g on g.id = gf.genre_id
    LEFT JOIN content.person_film_work pfw on pfw.film_work_id = fw.id
    LEFT JOIN content.person p on p.id = pfw.person_id
    GROUP BY fw.id
    HAVING greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at)) > '2021-06-16 20:14:09.256023 +00:00'
    ORDER BY greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at))
    LIMIT 3
    ) as t
