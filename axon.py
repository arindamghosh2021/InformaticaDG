from datetime import datetime as dt
from datetime import date
import datetime
import psycopg2 as pg #install
import pandas as pd #install
import streamlit as st #install
import os
import altair as alt
import warnings

############### ONE TIME CONFIGURATION ###########################################
Axon_hostname= "<Axon IP>"
postgres_port= "<Postgres db port>"

##################################################################################

## Ignore warnings showing in the terminal
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

####################################### HEADING ############################################

st.markdown("[![Foo](https://www.informatica.com/content/dam/informatica-com/en/images/gl01/informatica-logo.png)](https://www.informatica.com/)")
original_title = '<p style="font-family:sans-serif; color:Orange; font-size: 45px;">Axon Usage Statistics</p>'

st.markdown(original_title, unsafe_allow_html=True)
st.header('*This accelerator gives an option to view and download Axon usage statistics. This covers facet usage, User Login and Top CR Creator statistics*')


st.subheader('Please select the start and end date for the usage report')
today = datetime.date.today()
start_date = st.date_input('Start date', today)
end_date = st.date_input('End date',today)
delta=end_date-start_date
date_range=delta.days


username = st.text_input("Enter user name")
password = st.text_input("Enter password", type="password")

if "submitted" not in st.session_state:
    st.session_state.submitted = False

## Connect to postgres database with Axon hostname/IP, port and database name (always matviewdb)
if st.button('Submit') or st.session_state.submitted:
    st.session_state.submitted = True
    if start_date <= end_date:               
        try:
            conn = pg.connect(user=username,
            password=password,
            host=Axon_hostname,
            port=postgres_port,
            database="matviewdb")
        except (Exception, pg.Error) as error :
            st.error ("Error while connecting to PostgreSQL due to the below error")
            st.write(error)
            st.stop()
        
##################################  QUERY BUILDING FOR FACET OBJECT CREATION/MODIFICATION  ######################################

        query_facet = """select * from obj_facet_stats where date between %s and %s order by Date"""
        tuple_facet = (start_date, end_date)
        
        cur_facet = conn.cursor()
        cur_facet.execute(query_facet, tuple_facet)
        col_names = ['Date','CreateCount','UpdateCount','DeleteCount','FacetName']
        tab_facet = cur_facet.fetchall()
        cur_facet.close()
        
        df_facet = pd.DataFrame(tab_facet, columns=col_names)
        df_facet.fillna(value = 0, inplace = True)
        df_facet = df_facet[['Date', 'FacetName', 'CreateCount','UpdateCount','DeleteCount']]
        
##################################  QUERY BUILDING FOR USER LOGIN  ########################################

        query_usr_login = """ select * from usr_login_stats uls where date between %s and %s order by date """
        tuple_usr_login = (start_date, end_date)
        
        cur_usr_login = conn.cursor()
        cur_usr_login.execute(query_usr_login, tuple_usr_login)
        col_names = ['Date','LoginCount','Name','Email','Role']
        tab_usr_login = cur_usr_login.fetchall()
        cur_usr_login.close()
        
        df_usr_login = pd.DataFrame(tab_usr_login, columns=col_names)
        df_usr_login.fillna(value = 0, inplace = True)
        df_usr_login = df_usr_login[['Date', 'Name','Email','Role','LoginCount']]
        
##################################  QUERY BUILDING FOR TOP CR CREATORS  ########################################
        
        query_cr_creator = """ select * from cr_top_creators """
        
        cur_cr_creator = conn.cursor()
        cur_cr_creator.execute(query_cr_creator)
        col_names = ['Name','Email','Role','CRCount','Rank']
        tab_cr_creators = cur_cr_creator.fetchall()
        cur_cr_creator.close()
        
        df_cr_creators = pd.DataFrame(tab_cr_creators, columns=col_names)
        df_cr_creators.fillna(value = 0, inplace = True)
        
################################  CHART CREATION  FOR USER LOGIN  ################################################
        
        st.subheader('User Login Statistics visual representation')
        
        df_usr_login[['Date']] = df_usr_login[['Date']].apply(pd.to_datetime)
        
        if df_usr_login.empty:
            st.info('No user login details available for the given date range')
        else:
            chart = alt.Chart(df_usr_login).mark_bar().encode(x=alt.X('Date:O', timeUnit='monthdate', axis=alt.Axis(tickCount=date_range, labelAngle=-90), title='Date'), y= alt.Y('LoginCount:Q',axis=alt.Axis(tickMinStep=10)), color='Name',tooltip=['Name', 'LoginCount'])
        
            st.altair_chart(chart, use_container_width=True)
        
################################  CHART CREATION  FOR FACET OBJECT CREATION  ################################################        
        st.subheader('Facet Object Creation Statistics visual representation')
        df_facet[['Date']] = df_facet[['Date']].apply(pd.to_datetime)
        df_facet_create = df_facet[df_facet.CreateCount !=0]
        if df_facet_create.empty:
            st.info('No creation for any facet was done for the given date range')
        else:
            chart = alt.Chart(df_facet_create).mark_bar().encode(y=alt.Y('Date:O', timeUnit='monthdate', axis=alt.Axis(tickCount=date_range), title='Date'), x= alt.X('CreateCount:Q'), color='FacetName',tooltip=['FacetName', 'CreateCount']).resolve_scale(x='independent')
        
            st.altair_chart(chart, use_container_width=True)
        
################################  CHART CREATION  FOR FACET OBJECT UPDATE  ################################################            
        st.subheader('Facet Object Update Statistics visual representation')
        df_facet_update = df_facet[df_facet.UpdateCount !=0]
        if df_facet_update.empty:
            st.info('No update for any facet was done for the given date range')
        else:
            chart = alt.Chart(df_facet_update).mark_bar().encode(y=alt.Y('Date:O', timeUnit='monthdate', axis=alt.Axis(tickCount=date_range), title='Date'), x= alt.X('UpdateCount:Q'), color='FacetName',tooltip=['FacetName', 'UpdateCount']).resolve_scale(x='independent')
        
            st.altair_chart(chart, use_container_width=True)
        
################################  CHART CREATION  FOR FACET OBJECT DELETION  ################################################           
        st.subheader('Facet Object Delete Statistics visual representation')
        df_facet_delete = df_facet[df_facet.DeleteCount !=0]
        if df_facet_delete.empty:
            st.info('No deletion for any facet was done for the given date range')
        else:
            chart = alt.Chart(df_facet_delete).mark_bar().encode(y=alt.Y('Date:O', timeUnit='monthdate', axis=alt.Axis(tickCount=date_range), title='Date'), x= alt.X('DeleteCount:Q'), color='FacetName',tooltip=['FacetName', 'DeleteCount']).resolve_scale(x='independent')
        
            st.altair_chart(chart, use_container_width=True)
        
        st.subheader('Top CR Creators visual representation')
        st.write('This is for all the CRs created till date and not dependent on the date range')
        
        if df_cr_creators.empty:
            st.info('No CR has been created so far')
        else:
            chart = alt.Chart(df_cr_creators).mark_bar().encode(y=alt.Y('Name:O', title='User Name'), x= alt.X('CRCount:Q', title = 'CR Creation Count'), color='Role',tooltip=['CRCount','Rank']).resolve_scale(x='independent')
        
            st.altair_chart(chart, use_container_width=True)
            
######################################## EXCEL REPORT GENERATION ####################################################
        
        st.subheader('Click to download the report in excel format')
        if st.button('Download'):
            st.session_state.submitted = False
            st.info('Please wait....The report will be downloaded now for the date range: `%s` to `%s`.'% (start_date, end_date))
            now = dt.now()
            dt_string = now.strftime("%Y%m%d%H%M%S")
            
            
            path = os.getcwd()
            filename = "AxonUsageStats_"+dt_string+".xlsx"
            
            fullpath = path+ '\\'+filename
            
            writer = pd.ExcelWriter(fullpath, engine='xlsxwriter',options = {'strings_to_numbers': True})
            df_facet.to_excel(writer, index=False, sheet_name='FacetUsageStats')
            df_usr_login.to_excel(writer, index=False, sheet_name='UserLoginStats')
            df_cr_creators.to_excel(writer, index=False, sheet_name='TopCRCreators')
            writer.save()
            writer.close()
            st.write('Report has been downloaded in the path:',path)
            
    else:
        st.error('Error: End date must be greater than start date.')
################################################################################# 