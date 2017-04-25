# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 14:41:34 2017

@author: peshalnayak
"""

import sys
import csv
from collections import defaultdict
import psycopg2
import pandas as pd

def create_tables(connection_command):
    conn = None
    try:
        conn = psycopg2.connect(connection_command)
        cur = conn.cursor()
        
        cur.callproc('CreateTables')
        
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def input_from_csv(filename,connection_command):
    
    # number of columns corresponding to each table
    table_col_nums = {'*Org' : 3, '*Meet' : 4, '*Participant' : 4, '*Leg' : 1, '*Stroke' : 1, '*Distance' : 1, '*Event' : 3, '*StrokeOf' : 3, '*Heat' : 3, '*Swim' : 6}
    
    # read data from the table into a variable
    csv_data = defaultdict(list)
    with open(filename, 'r') as csv_file:
        # filter empty lines
        csv_reader = csv.reader(filter(lambda l: l.strip(',\n'), csv_file))
    
        header = None
        for row in csv_reader:
            if row[0].startswith('*'):
                header = row[0]
            else:
                # take out the empty columns that show up for some tables
                csv_data[header].append(row[:table_col_nums[header]])
        
    # open connection
    conn = None
    try:
        conn = psycopg2.connect(connection_command)
        cur = conn.cursor()
    
        # call different sql procedures and input data
        
        # Org
        for col in range(0,len(csv_data['*Org'])):
            cur.callproc('Add_to_org',(csv_data['*Org'][col][0],csv_data['*Org'][col][1],csv_data['*Org'][col][2]))
        
        #stroke
        for col in range(0,len(csv_data['*Stroke'])):
            cur.callproc('Add_to_stroke',(csv_data['*Stroke'][col][0],))        

        #distance
        for col in range(0,len(csv_data['*Distance'])):
            cur.callproc('Add_to_distance',(csv_data['*Distance'][col][0],))
        
        # leg
        for col in range(0,len(csv_data['*Leg'])):
            cur.callproc('Add_to_leg',(csv_data['*Leg'][col][0],))            
            
        # meet
        for col in range(0,len(csv_data['*Meet'])):
            cur.callproc('Add_to_meet',(csv_data['*Meet'][col][0], csv_data['*Meet'][col][1], csv_data['*Meet'][col][2], csv_data['*Meet'][col][3]))

        # participant
        for col in range(0,len(csv_data['*Participant'])):
            cur.callproc('Add_to_participant',(csv_data['*Participant'][col][0], csv_data['*Participant'][col][1], csv_data['*Participant'][col][2], csv_data['*Participant'][col][3]))
                    
        # event
        for col in range(0,len(csv_data['*Event'])):
            cur.callproc('Add_to_event',(csv_data['*Event'][col][0], csv_data['*Event'][col][1], csv_data['*Event'][col][2]))
        
        # heat
        for col in range(0,len(csv_data['*Heat'])):
            cur.callproc('Add_to_heat',(csv_data['*Heat'][col][0], csv_data['*Heat'][col][1], csv_data['*Heat'][col][2]))  
        
        # swim
        for col in range(0,len(csv_data['*Swim'])):
            cur.callproc('Add_to_swim',(csv_data['*Swim'][col][0], csv_data['*Swim'][col][1], csv_data['*Swim'][col][2], csv_data['*Swim'][col][3], csv_data['*Swim'][col][4], csv_data['*Swim'][col][5]))  
            
        # stroke_of
        for col in range(0,len(csv_data['*StrokeOf'])):
            cur.callproc('Add_to_strokeof',(csv_data['*StrokeOf'][col][0], csv_data['*StrokeOf'][col][1], csv_data['*StrokeOf'][col][2])) 
        
        # close connection                
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def print_table(table_name,filename,connection_command):
        
    conn = psycopg2.connect(connection_command)
    cur = conn.cursor()
#    table_name = 'Org'
    write_command1 = '*' + table_name + '\n' 
#    fd = open('resultsfile.csv','a')
    fd = open(filename,'a')
    
    fd.write(write_command1)
    fd.close()
    
    write_command2 = 'COPY ' + table_name + ' TO STDOUT with csv'
    with open(filename, 'a') as f:
        cur.copy_expert(write_command2,f)
        
    conn.close()
    
def print_all_tables(filename,connection_command):
    ''' For printing all the tables we will loop through each table and print its contents'''
    table_names = ['Org', 'Meet', 'Participant', 'Leg', 'Stroke', 'Distance', 'Event', 'StrokeOf', 'Heat', 'Swim']
#    query = ("""SELECT print_ """)

    for table_name in table_names:
        print_table(table_name,filename,connection_command)

def take_user_input(connection_command):
    
    user_choice = input("which table do you want to update? Please enter choice number (eg. 1 for choice 1) \n 1. org table \n 2. stroke table \n 3. distance table \n 4. leg table \n 5. meet table \n 6. participant table \n 7. event table \n 8. heat table \n 9. swim table \n 10. strokeof table \n")
    conn = psycopg2.connect(connection_command)
    cur = conn.cursor()
    
    if (user_choice == "1"):
    # org table
        o_id = input("Enter org id (4 character). Eg: U422 \n")
        o_name = input("Enter org name (50 character). Eg: UTAustin: \n")
        
        if ";" in o_name:
            print("Something wrong with org name. Please try again \n")
            return
        
        is_univ = input("is organization a university? (t/f) \n")
        
        try:
            cur.callproc('Add_to_org',(o_id,o_name,is_univ))
        except Exception as e:
            print(e)
        cur.close()
        conn.commit()
    
    elif (user_choice == "2"):
        # stroke table
        my_stroke = input("Enter a stroke name (50 characters). Eg: Freestyle \n ")
        
        if ";" in my_stroke:
            print("Something wrong with stroke name. Please try again \n")
            return
        try:
            cur.callproc('Add_to_stroke',(my_stroke,))
        except Exception as e:
            print(e)
        cur.close()
        conn.commit()
            
    elif (user_choice == "3"):
        # distance table
        my_dist = input("Enter a distance value in meters (eg. 100) \n")
        
        try:
            cur.callproc('Add_to_distance',(my_dist,))
        except Exception as e:
            print(e)
        cur.close()
        conn.commit()
        
    elif (user_choice == "4"):
        # leg table
        leg_val = input("Enter a leg value (1/2/3/4) \n")
        
        if int(leg_val) not in [1,2,3,4]:
            print("Wrong value. please try again \n")
            return

        try:
            cur.callproc('Add_to_leg',(leg_val,))
        except Exception as e:
            print(e) 

        cur.close()
        conn.commit()            
        
    elif (user_choice == "5"):
        # meet table
        
        m_name = input("Enter a meet name (50 character). eg: NCAA_Summer \n")
        
        if ";" in m_name:
            print("Something wrong with meet name. Please try again \n")
            return
            
        start_date = input("Enter start date (format: mm/dd/yyyy)\n")
        
        num_days = input("Enter number of days \n")
        
        o_id = input("Enter org id (4 character). Eg: U422 \n")
            
        try:
            cur.callproc('Add_to_meet',(m_name, start_date, num_days, o_id))
        except Exception as e:
            print(e) 

        cur.close()
        conn.commit()            
            
    elif (user_choice == "6"):
        # participant table
        p_id = input("Enter participant id (Eg: P187734) \n")
        pp_gender = input("Enter participant gender (M/F) \n")
        o_id = input("Enter org id (4 character). Eg: U422 \n")
        p_name = input("Enter participant name (50 character) \n")
        
        if ";" in p_id:
            print("Something wrong with org name. Please try again \n")
            return
        
        if ";" in p_name:
            print("Something wrong with org name. Please try again \n")
            return
        
        try:
            cur.callproc('Add_to_participant',(p_id, pp_gender, o_id, p_name))
        except Exception as e:
            print(e)         

        cur.close()
        conn.commit()            
            
    elif (user_choice == "7"):
        # event table
        e_id = input("Enter an event id (Eg. E0107) \n")
        e_gender = input("Enter event gender (M/F) \n")
        e_dist = input("Enter a distance value in meters (eg. 100) \n")
        
        if ";" in e_id:
            print("Something wrong with org name. Please try again \n")
            return
        
        try:
            cur.callproc('Add_to_event',(e_id, e_gender, e_dist))
        except Exception as e:
            print(e)         

        cur.close()
        conn.commit()            
            
    elif (user_choice == "8"):
        # heat table
        h_id = input("Input an integer heat id \n")
        he_id = input("Enter an event id (Eg. E0107) \n")
        hm_name = input("Enter a meet name (50 character). eg: NCAA_Summer \n")
        
        if ";" in hm_name:
            print("Something wrong with org name. Please try again \n")
            return
        
        try:
            cur.callproc('Add_to_heat',(h_id, he_id, hm_name))
        except Exception as e:
            print(e) 

        cur.close()
        conn.commit()            
            
    elif (user_choice == "9"):
        # swim table
        sh_id = input("Input an integer heat id \n")
        se_id = input("Enter an event id (Eg. E0107) \n")
        sm_name = input("Enter a meet name (50 character). eg: NCAA_Summer \n")
        sp_id = input("Enter participant id (Eg: P187734) \n")
        sleg_val = input("Enter a leg value (1/2/3/4) \n")
        smy_time = input("Enter a value for time in seconds \n")
        
        if ";" in sm_name:
            print("Something wrong with org name. Please try again \n")
            return
            
        if ";" in sp_id:
            print("Something wrong with org name. Please try again \n")
            return
        
        if(smy_time == ""):
            smy_time = "1000000000000000000"
        
        try:
            cur.callproc('Add_to_swim',(sh_id, se_id, sm_name, sp_id, sleg_val, smy_time))
        except Exception as e:
            print(e) 

        cur.close()
        conn.commit()            
            
    elif (user_choice == "10"):
        # strokeof table
        se_id = input("Enter an event id (Eg. E0107) \n")
        sleg_val = input("Enter a leg value (1/2/3/4) \n")
        smy_stroke = input("Enter a stroke name (50 characters). Eg: Freestyle \n")
        
        if ";" in smy_stroke:
            print("Something wrong with org name. Please try again \n")
            return
        
        try:
            cur.callproc('Add_to_strokeof',(se_id, sleg_val, smy_stroke))
        except Exception as e:
            print(e)        

        cur.close()
        conn.commit()            
            
    else:
        print("Please enter a valid choice")
        return    
            
if __name__ == '__main__':  
    filename = 'sample_data.csv'
    
    
#    conn = psycopg2.connect("dbname=postgres user=postgres password=qwertrpoiuyu")
#    cur = conn.cursor()
#    
#    my_val = "garbage"
#    
#    while(1):
#        choice = input("Enter choice")
#        
#        if(choice == "1"):
#            try: 
#                cur.callproc('Add_to_leg',(my_val,))
#                #        return user
#            except Exception as e:
#                print(str(e))
#        else:
#            break
#        return None
    
#    create_tables()
#    input_from_csv(filename)
    #print_all_tables()
    
    #user input for org participant table
    
    
    psswd = input("Enter password")
    connection_command = "dbname=postgres user=postgres password=" + psswd
    create_tables(connection_command)
    
    while(1):
        user_choice = input("What would you like to do? Please enter option number (eg: 1 for choice 1) \n 1. Read data from a csv file \n 2. Take user input for tables \n 3. Save data to a file \n 4. For a Meet, display a Heat Sheet \n 5. For a Participant and Meet, display a Heat Sheet limited to just that swimmer \n 6. For a School and Meet, display a Heat Sheet limited to just that School’s swimmers \n 7. For a School and Meet, display just the names of the competing swimmers \n 8. For an Event and Meet, display all results sorted by time \n 9. For a Meet, display the scores of each school \n 10. exit \n Your choice:")
        
        if(user_choice == "1"):
            
            print("Load data from csv \n")
            filename = input("Enter the name of the csv file with .csv extension(eg: data.csv) \n")
            if ".csv" not in filename:
                print("Invalid input file. Enter the name with the csv extension (eg: data.csv) \n")
                continue
            else:
                input_from_csv(filename,connection_command)
        
        elif(user_choice == "2"):
            
            take_user_input(connection_command)
            
        elif(user_choice == "3"):
            
            filename = input("Enter output csv filename (eg: results.csv)")
            print_all_tables(filename, connection_command)
            
        elif(user_choice == "4"):
        
            meet_name = input("Enter meet name")
            
            if ";" in meet_name:
                print("Something wrong with meet name. Please try again \n")
                break
            
            conn = psycopg2.connect(connection_command)
            cur = conn.cursor()
            cur.callproc('event_info',(meet_name,))
            my_table = pd.read_sql('select * from heatsheet_table', conn)
            print(my_table)
            #cur.copy_to(sys.stdout,'heatsheet_table',sep = '\t')
        
        elif(user_choice == "5"):
            
            meet_name = input("Enter meet name")
            swimmer_name = input("Enter swimmer id")
            
            if ";" in meet_name:
                print("Something wrong with meet name. Please try again \n")
                break
            
            if ";" in swimmer_name:
                print("Something wrong with swimmer name. Please try again \n")
                break

            conn = psycopg2.connect(connection_command)
            cur = conn.cursor()
            cur.callproc('event_info_single_swimmer',(meet_name, swimmer_name))
            my_table = pd.read_sql('select * from single_swimmer_heatsheet', conn)
            print(my_table)
            #cur.copy_to(sys.stdout,'single_swimmer_heatsheet',sep = '\t') 
            
        elif(user_choice == "6"):
            
            meet_name = input("Enter meet name")
            school_id = input("Enter school's org id")
            
            if ";" in meet_name:
                print("Something wrong with meet name. Please try again \n")
                break
            
            if ";" in school_id:
                print("Something wrong with school's org id. Please try again \n")
                break

            conn = psycopg2.connect(connection_command)
            cur = conn.cursor()
            cur.callproc('event_info_school_heatsheet',(meet_name, school_id))
            my_table = pd.read_sql('select * from school_heatsheet', conn)
            print(my_table)            
            
            #cur.copy_to(sys.stdout,'school_heatsheet',sep = '\t')            
            
        elif(user_choice == "7"):
            
            meet_name = input("Enter meet name")
            school_id = input("Enter school's org id")
            
            if ";" in meet_name:
                print("Something wrong with meet name. Please try again \n")
                break
            
            if ";" in school_id:
                print("Something wrong with school's org id. Please try again \n")
                break            
            
            conn = psycopg2.connect(connection_command)
            cur = conn.cursor()
            cur.callproc('school_swimmers',(meet_name, school_id))
            my_table = pd.read_sql('select * from school_swimmers_table', conn)
            print(my_table)
            #cur.copy_to(sys.stdout,'school_swimmers_table',sep = '\t')            
        
        elif(user_choice == "8"):
            
            meet_name = input("Enter meet name")
            event_id = input("Enter event id")

            if ";" in meet_name:
                print("Something wrong with meet name. Please try again \n")
                break
            
            if ";" in event_id:
                print("Something wrong with event id. Please try again \n")
                break             

            conn = psycopg2.connect(connection_command)
            cur = conn.cursor()
            cur.callproc('event_info_time_sorted',(meet_name, event_id))
            my_table = pd.read_sql('select * from time_sorted_table', conn)
            print(my_table)
            #cur.copy_to(sys.stdout,'time_sorted_table',sep = '\t')      
        
        elif(user_choice == "9"):
            
            meet_name = input("Enter meet name")
            
            if ";" in meet_name:
                print("Something wrong with meet name. Please try again \n")
                break

            conn = psycopg2.connect(connection_command)
            cur = conn.cursor()
            cur.callproc('event_info_scored',(meet_name,))
            #cur.copy_to(sys.stdout,'team_totals',sep = '\t') 
            my_table = pd.read_sql('select * from team_totals', conn)
            print(my_table)             
            
        elif(user_choice == "10"):
            print("Goodbye!")
            break
        else:
            print("Please enter the appropriate choice number")
