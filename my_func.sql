CREATE OR REPLACE FUNCTION CreateTables()
RETURNS VOID
AS $$
    BEGIN
        DROP TABLE IF EXISTS strokeof;
        DROP TABLE IF EXISTS swim;
        DROP TABLE IF EXISTS heat;
        DROP TABLE IF EXISTS event;
        DROP TABLE IF EXISTS participant;
        DROP TABLE IF EXISTS meet;
        DROP TABLE IF EXISTS org;
        DROP TABLE IF EXISTS leg;
        DROP TABLE IF EXISTS stroke;
        DROP TABLE IF EXISTS distance;

        CREATE TABLE org(
            o_id VARCHAR(4),
            o_name VARCHAR(50) NOT NULL,
            is_univ boolean NOT NULL,
            PRIMARY KEY(o_id)
        );

        CREATE TABLE stroke(
            stroke VARCHAR(50),
            PRIMARY KEY(stroke)
        );

        CREATE TABLE distance(
            dist INT CHECK (dist > 0),
            PRIMARY KEY(dist)
        );

        CREATE TABLE leg(
            leg INT,
            PRIMARY KEY(leg)
        );

        CREATE TABLE meet(
            m_name varchar(50),
            start_date date NOT NULL,
            num_days INT NOT NULL,
            o_id VARCHAR(4) NOT NULL,
            PRIMARY KEY(m_name),
            FOREIGN KEY (o_id) REFERENCES org(o_id)
        );

        CREATE TABLE participant(
            p_id VARCHAR(10),
            p_name VARCHAR(50) NOT NULL,
            gender VARCHAR(1) NOT NULL, -- add a constraint to check that this is M or F
            o_id VARCHAR(4) NOT NULL,
            PRIMARY KEY(p_id),
            FOREIGN KEY(o_id) REFERENCES org(o_id),
            CHECK (gender IN ('M', 'F'))
        );

        CREATE TABLE event(
            e_id VARCHAR(10),
            e_gender VARCHAR(1) NOT NULL,
            distance INT NOT NULL,
            PRIMARY KEY(e_id),
            FOREIGN KEY(distance) REFERENCES distance(dist),
            CHECK (e_gender IN ('M', 'F')),
            CHECK (distance > 0)
        );

        CREATE TABLE heat(
            h_id INT NOT NULL,
            e_id VARCHAR(10) NOT NULL,
            meet_name VARCHAR(50) NOT NULL,
            UNIQUE(h_id,e_id,meet_name),
            PRIMARY KEY(h_id,e_id,meet_name),
            FOREIGN KEY (e_id) REFERENCES event(e_id),
            FOREIGN KEY (meet_name) REFERENCES meet(m_name)
        );

        CREATE TABLE swim(
            h_id INT,
            e_id VARCHAR(10) NOT NULL,
            meet_name VARCHAR(50) NOT NULL,
            p_id VARCHAR(10) NOT NULL,
            leg INT,
            s_time FLOAT,
            PRIMARY KEY (h_id, e_id, meet_name, p_id),
            FOREIGN KEY (leg) REFERENCES leg(leg),
            FOREIGN KEY (h_id, e_id, meet_name) REFERENCES heat(h_id, e_id, meet_name),
            FOREIGN KEY (meet_name) REFERENCES meet(m_name),
            FOREIGN KEY (p_id) REFERENCES participant(p_id)
        );

        CREATE TABLE strokeof(
            e_id VARCHAR(10) NOT NULL,
            leg INT NOT NULL,
            stroke VARCHAR(50) NOT NULL,
            PRIMARY KEY(e_id, leg),
            FOREIGN KEY (e_id) REFERENCES event(e_id),
            FOREIGN KEY (leg) REFERENCES leg(leg),
            FOREIGN KEY (stroke) REFERENCES stroke(stroke)
        );
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION DropTables()
RETURNS VOID
AS $$
    BEGIN
        DROP TABLE IF EXISTS strokeof;
        DROP TABLE IF EXISTS swim;
        DROP TABLE IF EXISTS heat;
        DROP TABLE IF EXISTS event;
        DROP TABLE IF EXISTS participant;
        DROP TABLE IF EXISTS meet;
        DROP TABLE IF EXISTS org;
        DROP TABLE IF EXISTS leg;
        DROP TABLE IF EXISTS stroke;
        DROP TABLE IF EXISTS distance;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_org(my_o_id VARCHAR(4), my_o_name VARCHAR(50), my_is_univ boolean)
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO org VALUES (my_o_id, my_o_name, my_is_univ) 
        ON CONFLICT (o_id) DO UPDATE SET o_name = EXCLUDED.o_name, is_univ  = EXCLUDED.is_univ;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_stroke(my_stroke VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO stroke VALUES (my_stroke)
		ON CONFLICT (stroke) DO NOTHING;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_distance(my_dist INT)
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO distance VALUES (my_dist)
		ON CONFLICT (dist) DO NOTHING;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_leg(my_leg INT)
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO leg VALUES (my_leg)
		ON CONFLICT (leg) DO NOTHING;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_meet(my_m_name varchar(50), start_date date, num_days INT, o_id VARCHAR(4))
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO meet VALUES (my_m_name, start_date, num_days, o_id)
        ON CONFLICT (m_name) DO UPDATE SET start_date = EXCLUDED.start_date, num_days  = EXCLUDED.num_days, o_id = EXCLUDED.o_id;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_participant(my_p_id VARCHAR(10), gender VARCHAR(1), o_id VARCHAR(4), p_name VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO participant VALUES (my_p_id, p_name, gender, o_id)
        ON CONFLICT (p_id) DO UPDATE SET p_name = EXCLUDED.p_name, gender = EXCLUDED.gender, o_id = EXCLUDED.o_id;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_event(my_e_id VARCHAR(10), e_gender VARCHAR(1), distance INT)
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO event VALUES (my_e_id, e_gender, distance)
        ON CONFLICT (e_id) DO UPDATE SET e_gender = EXCLUDED.e_gender, distance = EXCLUDED.distance;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_heat(my_h_id INT, my_e_id VARCHAR(10), my_meet_name VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO heat VALUES (my_h_id, my_e_id, my_meet_name)
        ON CONFLICT (h_id, e_id, meet_name) DO NOTHING;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_swim(my_h_id INT, my_e_id VARCHAR(10), my_meet_name VARCHAR(50), my_p_id VARCHAR(10), my_leg INT, my_tyme FLOAT)
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO swim VALUES (my_h_id, my_e_id, my_meet_name, my_p_id, my_leg, my_tyme)
        ON CONFLICT (h_id, e_id, meet_name, p_id) DO UPDATE SET leg = EXCLUDED.leg, s_time = EXCLUDED.s_time;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Add_to_strokeof(my_e_id VARCHAR(10), my_leg INT, my_stroke VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        INSERT INTO strokeof VALUES (my_e_id, my_leg, my_stroke)
        ON CONFLICT (e_id, leg) DO UPDATE SET stroke = EXCLUDED.stroke;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_org(my_o_id VARCHAR(4))
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM org
        WHERE o_id = my_o_id;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_stroke(my_stroke VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM stroke
        WHERE stroke = my_stroke;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_distance(my_dist INT)
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM distance
        WHERE dist = my_dist;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_leg(my_leg INT)
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM leg
        WHERE leg = my_leg;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_meet(my_m_name varchar(50))
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM meet
        WHERE m_name = my_m_name;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_participant(my_p_id VARCHAR(10))
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM participant
        WHERE p_id = my_p_id;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_event(my_e_id VARCHAR(10))
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM event
        WHERE e_id = my_e_id;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_heat(my_h_id INT, my_e_id VARCHAR(10), my_meet_name VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM heat
        WHERE h_id = my_h_id AND e_id = my_e_id AND meet_name = my_meet_name;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_swim(my_h_id INT, my_e_id VARCHAR(10), my_meet_name VARCHAR(50), my_p_id VARCHAR(10))
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM swim
        WHERE h_id = my_h_id AND e_id = my_e_id AND meet_name = my_meet_name AND p_id = my_p_id;
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION Delete_from_strokeof(my_e_id VARCHAR(10), my_leg INT)
RETURNS VOID
AS $$
    BEGIN
        DELETE FROM strokeof
        WHERE e_id = my_e_id AND leg = my_leg;
    END $$
LANGUAGE plpgsql
VOLATILE;

/* 
\i /Users/christianburkhartsmeyer/comp430/project6/event_info.sql
SELECT event_info('meet1');
SELECT event_info('meet1');SELECT * FROM heatsheet_table;
*/

CREATE OR REPLACE FUNCTION single_event_heatsheet_nonrelay(meet_id VARCHAR(50), given_event_id VARCHAR(10))
RETURNS TABLE (h_id INT, p_name VARCHAR(50), s_time FLOAT, o_name VARCHAR(50), rank INT)
AS $$
    BEGIN
        DROP TABLE IF EXISTS best_times;
        CREATE TABLE best_times(
            p_id VARCHAR(10),
            s_time FLOAT
        );

        INSERT INTO best_times
        SELECT DISTINCT s.p_id, MIN(s.s_time)
        FROM swim s
        WHERE s.e_id=given_event_id AND s.meet_name=meet_id
        GROUP BY s.p_id;

        RETURN QUERY(
        SELECT s.h_id, p.p_name, s.s_time, o.o_name, CAST (r.rank AS INT)
        FROM swim s
        LEFT JOIN (
            SELECT b.p_id, b.s_time, ROW_NUMBER() OVER(ORDER BY b.s_time ASC) AS rank
            FROM best_times b
        ) r ON r.p_id=s.p_id AND r.s_time=s.s_time
        INNER JOIN participant p ON p.p_id=s.p_id
        INNER JOIN org o ON o.o_id=p.o_id
        WHERE s.e_id=given_event_id AND s.meet_name=meet_id
        ORDER BY s.h_id
        );
    END $$
LANGUAGE plpgsql
VOLATILE;

CREATE OR REPLACE FUNCTION single_event_heatsheet_relay(meet_id VARCHAR(50), given_event_id VARCHAR(10))
RETURNS TABLE (h_id INT, p_name VARCHAR(50), s_time FLOAT, leg INT, o_name VARCHAR(50), rank INT, team_time FLOAT)
AS $$
    BEGIN

    DROP TABLE IF EXISTS team_heat_times;
    CREATE TABLE team_heat_times(
        o_name VARCHAR(50),
        o_id VARCHAR(10),
        h_id INT,
        s_time FLOAT
    );

    INSERT INTO team_heat_times
        SELECT o.o_name, o.o_id, s.h_id, SUM(s.s_time)
        FROM swim s
        INNER JOIN participant p ON p.p_id=s.p_id
        INNER JOIN org o ON o.o_id=p.o_id
        WHERE s.e_id=given_event_id AND s.meet_name=meet_id
        GROUP BY s.h_id, o.o_name, o.o_id;

    DROP TABLE IF EXISTS best_times;
    CREATE TABLE best_times(
        o_name VARCHAR(50),
        o_id VARCHAR(10),
        h_id INT,
        s_time FLOAT
    );

    /* Gets the best heats from each team */
    INSERT INTO best_times
        SELECT s1.o_name, s1.o_id, s1.h_id, s1.s_time
        FROM team_heat_times s1
        INNER JOIN(
            SELECT DISTINCT tht.o_name, MIN(tht.s_time) AS s_time
            FROM team_heat_times tht
            GROUP BY tht.o_name
        )s2 ON s1.s_time=s2.s_time AND s1.o_name = s2.o_name;

    /* Return each of the heats, w/ each entry containing info about team and individual */
    RETURN QUERY(
    SELECT s.h_id, p.p_name, s.s_time, s.leg, o.o_name, CAST (r.rank AS INT), tht.s_time AS group_time
    FROM swim s
    INNER JOIN participant p ON p.p_id=s.p_id
    INNER JOIN org o ON o.o_id=p.o_id
    INNER JOIN team_heat_times tht ON tht.o_id=p.o_id AND tht.h_id=s.h_id
    LEFT JOIN (
        SELECT b.o_id, b.o_name, b.h_id, b.s_time, ROW_NUMBER() OVER(ORDER BY b.s_time ASC) AS rank
        FROM best_times b
    ) r ON r.o_id=p.o_id AND r.h_id=s.h_id
    WHERE s.e_id=given_event_id AND s.meet_name=meet_id
    ORDER BY s.h_id, p.o_id, s.leg
    );
    END $$
LANGUAGE plpgsql
VOLATILE;


CREATE OR REPLACE FUNCTION event_info(meet_id VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        DROP TABLE IF EXISTS event_info;
        CREATE TABLE event_info(
            event_id VARCHAR(10),
            gender VARCHAR(1),
            distance INT,
            is_relay BOOL
        );

        DROP TABLE IF EXISTS heatsheet_table;
        CREATE TABLE heatsheet_table(
            event_id VARCHAR(10),
            heat_id INT,
            gender VARCHAR(1),
            distance INT,
            swimmer VARCHAR(50),
            school VARCHAR(50),
            individual_time FLOAT,
            individual_rank INT,
            team_time FLOAT,
            team_rank INT
        );
    
        INSERT INTO event_info
        SELECT e.e_id, e.e_gender, e.distance, MAX(s.leg)>1 AS is_relay
        FROM event e
        INNER JOIN swim s
        ON e.e_id=s.e_id AND s.meet_name=meet_id
        GROUP BY e.e_id;
        
        INSERT INTO heatsheet_table
        SELECT e.event_id, COALESCE(sehn.h_id, sehr.h_id), e.gender, e.distance, COALESCE(sehn.p_name, sehr.p_name), COALESCE(sehn.o_name, sehr.o_name), COALESCE(sehn.s_time, sehr.s_time), sehn.rank, sehr.team_time, sehr.rank
        FROM event_info e
        LEFT JOIN single_event_heatsheet_nonrelay(meet_id, e.event_id) sehn ON NOT e.is_relay
        LEFT JOIN single_event_heatsheet_relay(meet_id, e.event_id) sehr ON e.is_relay;
    END $$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION event_info_scored(meet_id VARCHAR(50))
RETURNS VOID
AS $$
    BEGIN
        /* Generate normal heatsheet first */
        PERFORM event_info(meet_id);
        
        /* Create table w/ ranking to point relationship */
        DROP TABLE IF EXISTS rank_to_pts_relay;
        CREATE TABLE rank_to_pts_relay(
            rank INT,
            pts FLOAT
        );
        DROP TABLE IF EXISTS rank_to_pts_solo;
        CREATE TABLE rank_to_pts_solo(
            rank INT,
            pts FLOAT
        );
        /* Values divided by 4 for relays, since each person in relay has an entry */
        INSERT INTO rank_to_pts_relay VALUES
            (1, 2.0),
            (2, 1.0),
            (3, 0.5);
        INSERT INTO rank_to_pts_solo VALUES
            (1, 6.0),
            (2, 4.0),
            (3, 3.0),
            (4, 2.0),
            (5, 1.0);
            
        /* Create table w/ pts instead of rankings */
        DROP TABLE IF EXISTS heatsheet_pts;
        CREATE TABLE heatsheet_pts(
            event_id VARCHAR(10),
            heat_id INT,
            swimmer VARCHAR(50),
            school VARCHAR(50),
            individual_pts INT,
            team_pts INT
        );
        
        INSERT INTO heatsheet_pts
        SELECT hs.event_id, hs.heat_id, hs.swimmer, hs.school, solo.pts, relay.pts
        FROM heatsheet_table hs
        LEFT JOIN rank_to_pts_relay relay ON relay.rank=hs.team_rank
        LEFT JOIN rank_to_pts_solo solo ON solo.rank=hs.individual_rank
        ORDER BY hs.school;

        /* Return pts summed by team */
        DROP TABLE IF EXISTS team_totals;
        CREATE TABLE team_totals(
            org VARCHAR(50),
            total_points FLOAT
        );
        
        INSERT INTO team_totals
        SELECT hp.school, SUM(COALESCE(hp.individual_pts, hp.team_pts))
        FROM heatsheet_pts hp
        GROUP BY hp.school;
    END $$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION event_info_single_swimmer(meet_id VARCHAR(50), swimmer_id VARCHAR(10))
RETURNS VOID
AS $$
    BEGIN
        /* Generate normal heatsheet first */
        PERFORM event_info(meet_id);
        
        /* Create a table w/ only the relevant results */
        DROP TABLE IF EXISTS single_swimmer_heatsheet;
        CREATE TABLE single_swimmer_heatsheet(
            event_id VARCHAR(10),
            heat_id INT,
            gender VARCHAR(1),
            distance INT,
            swimmer VARCHAR(50),
            school VARCHAR(50),
            individual_time FLOAT,
            individual_rank INT,
            team_time FLOAT,
            team_rank INT
        );
        
        INSERT INTO single_swimmer_heatsheet
        SELECT ht.event_id, ht.heat_id, ht.gender, ht.distance, ht.swimmer, ht.school, ht.individual_time, ht.individual_rank, ht.team_time, ht.team_rank
        FROM heatsheet_table ht
        INNER JOIN (
            SELECT p.p_name
            FROM participant p
            WHERE p.p_id=swimmer_id
        ) parts ON parts.p_name=ht.swimmer;
    END $$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION event_info_school_heatsheet(meet_id VARCHAR(50), school_id VARCHAR(4))
RETURNS VOID
AS $$
    BEGIN
        /*
        event_info_school_heatsheet(meetid, schoolid)
        school_heatsheet
        */
        /* Generate normal heatsheet first */
        PERFORM event_info(meet_id);
        
        /* Create a table w/ only the relevant results */
        DROP TABLE IF EXISTS school_heatsheet;
        CREATE TABLE school_heatsheet(
            event_id VARCHAR(10),
            heat_id INT,
            gender VARCHAR(1),
            distance INT,
            swimmer VARCHAR(50),
            school VARCHAR(50),
            individual_time FLOAT,
            individual_rank INT,
            team_time FLOAT,
            team_rank INT
        );

        INSERT INTO school_heatsheet
        SELECT ht.event_id, ht.heat_id, ht.gender, ht.distance, ht.swimmer, ht.school, ht.individual_time, ht.individual_rank, ht.team_time, ht.team_rank
        FROM heatsheet_table ht
        INNER JOIN (
            SELECT o.o_name
            FROM org o
            WHERE o.o_id=school_id
        ) orgs ON orgs.o_name=ht.school;
    END $$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION school_swimmers(meet_id VARCHAR(50), school_id VARCHAR(4))
RETURNS VOID
AS $$
    BEGIN
        /*
        school_swimmers(meetid, schoolid)
        school_swimmers_table
        */
        
        PERFORM event_info_school_heatsheet(meet_id, school_id);

        DROP TABLE IF EXISTS school_swimmers_table;
        CREATE TABLE school_swimmers_table(
            swimmer VARCHAR(50)
        );
        
        INSERT INTO school_swimmers_table
        SELECT DISTINCT sh.swimmer
        FROM school_heatsheet sh;
    END $$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION event_info_time_sorted(meet_id VARCHAR(50), g_event_id VARCHAR(10))
RETURNS VOID
AS $$
    BEGIN
        /*
        event_info_time_sorted(meetid, eventid)
        time_sorted_table
        */
        PERFORM event_info(meet_id);
        
        DROP TABLE IF EXISTS time_sorted_table;
        CREATE TABLE time_sorted_table(
            heat_id INT,
            gender VARCHAR(1),
            distance INT,
            swimmer VARCHAR(50),
            school VARCHAR(50),
            individual_time FLOAT,
            individual_rank INT,
            team_time FLOAT,
            team_rank INT
        );

        INSERT INTO time_sorted_table
        SELECT ht.heat_id, ht.gender, ht.distance, ht.swimmer, ht.school, ht.individual_time, ht.individual_rank, ht.team_time, ht.team_rank
        FROM heatsheet_table ht
        WHERE ht.event_id=g_event_id
        ORDER BY ht.team_time, ht.individual_time;

    END $$
LANGUAGE plpgsql;

