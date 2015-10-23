#!/usr/bin/python
import psycopg2
import pprint
import requests
import xmltodict, json
import urllib
import datetime
import csv
import sys, traceback
import pytz
from datetime import datetime
from dateutil import parser
import time
import threading

reload(sys);
sys.setdefaultencoding("utf-8");
 
conn_string = "dbname='sunedison' port='5439' user='XXXX' password='XXXX' host='sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com'";
print "Connecting to database";
conn = psycopg2.connect(conn_string);
print "Connected to database";

def startHere():
	modifydate = getLastModifiedTimestampForDatabase();
	systemTimestamp = getSystemTimestamp();
	differenceInMinutes = calculateDifferenceInTimestamps(systemTimestamp,modifydate);
	print "Time difference: %d" %differenceInMinutes;
	leadExtraction(differenceInMinutes);

def getLastModifiedTimestampForDatabase():
	cursor = conn.cursor();
	cursor.execute("SELECT modifydate FROM leads_dev ORDER BY modifydate DESC LIMIT 1");
	rows = cursor.fetchall();
	for row in rows:
		modifydate = row[0];
	return modifydate;	

def getSystemTimestamp():
	systemTimezone = pytz.timezone('America/Mazatlan')
	systemTimestampString = datetime.now(systemTimezone).strftime('%Y-%m-%d %H:%M:%S');
	systemTimestamp = datetime.strptime(systemTimestampString, '%Y-%m-%d %H:%M:%S');
	return systemTimestamp;

def calculateDifferenceInTimestamps(systemTimestamp, modifydate):
	d1_ts = time.mktime(systemTimestamp.timetuple())
	d2_ts = time.mktime(modifydate.timetuple())
	difference = int(((d1_ts-d2_ts) / 60) + 1)
	return difference

def leadExtraction(differenceInMinutes):
	url = "https://service.leads360.com/ClientService.asmx/GetLeadsSpan?username=XXXX@sunedison.com&password=XXXX?&fromNowMinutes=%s"%differenceInMinutes
	r = requests.get(url);
	output = xmltodict.parse(r.content);
	try:
		a = threading.Thread(target=getModifiedLeads,args=(output,));
		b = threading.Thread(target=getModifiedActionLogs,args=(output,));
	except:
		print "Exception";
		traceback.print_exc(file=sys.stdout);

	a.start()
	b.start()

def getModifiedLeads(output):
	firstname="";
	lastname="";
	home_address="";
	city="";
	state="";
	zip="";
	home_phone="";
	work_phone="";
	email="";
	average_monthly_electric_bill="";
	electricity_utility_company="";
	do_you_own_the_home="";
	roof_shade="";
	roof_type="";
	google_street_view_link="";
	sms_phone="";
	sms_opt_out="";
	netsuite_id="";
	lead_source="";
	lead_price_paid="";
	country="";
	duplicate_result_from_netsuite="";
	home_type="";
	preferred_method_of_follow_up="";
	lead_generator_comment="";
	lead_generator_date="";
	lead_generator_time="";
	updates_about_sunedison="";
	how_much_do_you_want_to_cut_your_bill="";
	homeowner_require_financing="";
	water_heater_type="";
	interested_in="";
	originator="";
	originator_email="";
	sunedison_employee_referrer_firstname="";
	sunedison_employee_referrer_lastname="";
	sunedison_employee_referrer_email="";
	does_your_house_have_an_attic="";
	do_you_have_a_central_heating_and_cooling_system="";
	primary_phone="";
	credit_score="";
	custom_1="";
	adressee="";
	total_system_size_echowatts="";
	total_retail_price_quoted="";
	contract_cancelled_date="";
	interested_in_purchase_type="";
	financing_program="";
	solar_designer="";
	lost_reason="";
	lost="";
	lost_cancel_comments="";
	homeowner_lease_contract_status="";
	homeowner_credit_check_status="";
	address_2="";
	proposal_status="";
	campaign_subcategory="";
	name="";
	address="";
	google_street_view_link1="";
	email1="";
	primary_phone1="";
	home_phone1="";
	mobile_phone1="";
	work_phone1="";
	international_phone="";
	netsuite_token_id="";
	advisor_notes="";
	solar_advisor="";
	kw_sold="";
	system_price="";
	energy_consultant="";
	energy_consultant_notes="";
	other_notes="";
	lead_source_manual="";
	designer_notes="";
	pricing_exceptions="";
	id="";
	collected_utility_bill="";
	contract_status="";
	createdate="";
	modifydate="";
	last_distribution_date="";
	lead_title="";
	status="";
	agent_name="";
	agent_email="";
	campaign="";

	if(output['Leads'] == ''):
		print "Empty"
	else:
		if isinstance(output["Leads"]["Lead"], list):
			for lead in output['Leads']['Lead']:
				for field in lead['Fields']['Field']:
					if field['@FieldTitle'] == 'First Name':
						firstname = field['@Value'];
					elif field['@FieldTitle'] == 'Last Name':
						lastname = field['@Value'];
					elif field['@FieldTitle'] == 'Home Address':
						home_address = field['@Value'];
					elif field['@FieldTitle'] == 'City':
						city = field['@Value'];
					elif field['@FieldTitle'] == 'State':
						state = field['@Value'];
					elif field['@FieldTitle'] == 'Zip':
						zip = field['@Value'];
					elif field['@FieldTitle'] == 'Home Phone':
						home_phone = field['@Value'];
					elif field['@FieldTitle'] == 'Work Phone':
						work_phone = field['@Value'];
					elif field['@FieldTitle'] == 'Email':
						email = field['@Value'];
					elif field['@FieldTitle'] == 'Average Monthly Electric Bill':
						average_monthly_electric_bill = field['@Value'];
					elif field['@FieldTitle'] == 'Electricity Utility Company':
						electricity_utility_company = field['@Value'];
					elif field['@FieldTitle'] == 'Do you own the home?':
						do_you_own_the_home = field['@Value'];
					elif field['@FieldTitle'] == 'Roof Shade':
						roof_shade = field['@Value'];
					elif field['@FieldTitle'] == 'Roof Type':
						roof_type = field['@Value'];
					elif field['@FieldTitle'] == 'Google Street View Link':
						google_street_view_link = field['@Value'];
					elif field['@FieldTitle'] == 'SMS Phone':
						sms_phone = field['@Value'];
					elif field['@FieldTitle'] == 'SMS Opt-Out':
						sms_opt_out = field['@Value'];
					elif field['@FieldTitle'] == 'Netsuite ID':
						netsuite_id = field['@Value'];
					elif field['@FieldTitle'] == 'Lead Source':
						lead_source = field['@Value'];
					elif field['@FieldTitle'] == 'Lead Price Paid ($)':
						lead_price_paid = field['@Value'];
					elif field['@FieldTitle'] == 'Country':
						country = field['@Value'];
					elif field['@FieldTitle'] == 'Duplicate Result from Netsuite':
						duplicate_result_from_netsuite = field['@Value'];
					elif field['@FieldTitle'] == 'Home Type':
						home_type = field['@Value'];
					elif field['@FieldTitle'] == 'Preferred Method of Follow-Up':
						preferred_method_of_follow_up = field['@Value'];
					elif field['@FieldTitle'] == 'Lead Generator Comment':
						lead_generator_comment = field['@Value'];
					elif field['@FieldTitle'] == 'Lead Generator Date':
						lead_generator_date = field['@Value'];
					elif field['@FieldTitle'] == 'Lead Generator Time':
						lead_generator_time = field['@Value'];
					elif field['@FieldTitle'] == 'Yes, I would like to receive updates about SunEdis':
						updates_about_sunedison = field['@Value'];
					elif field['@FieldTitle'] == 'How much do you want to cut your bill?':
						how_much_do_you_want_to_cut_your_bill = field['@Value'];
					elif field['@FieldTitle'] == 'Homeowner require financing?':
						homeowner_require_financing = field['@Value'];
					elif field['@FieldTitle'] == 'Water Heater Type':
						water_heater_type = field['@Value'];
					elif field['@FieldTitle'] == 'Interested In':
						interested_in = field['@Value'];
					elif field['@FieldTitle'] == 'Originator':
						originator = field['@Value'];
					elif field['@FieldTitle'] == 'Originator Email':
						originator_email = field['@Value'];
					elif field['@FieldTitle'] == 'SunEdison Employee Referrer First Name':
						sunedison_employee_referrer_firstname = field['@Value'];
					elif field['@FieldTitle'] == 'SunEdison Employee Referrer Last Name':
						sunedison_employee_referrer_lastname = field['@Value'];
					elif field['@FieldTitle'] == 'SunEdison Employee Referrer Email':
						sunedison_employee_referrer_email = field['@Value'];
					elif field['@FieldTitle'] == 'Does your house have an attic?':
						does_your_house_have_an_attic = field['@Value'];
					elif field['@FieldTitle'] == 'Do you have a central heating and cooling system?':
						do_you_have_a_central_heating_and_cooling_system = field['@Value'];
					elif field['@FieldTitle'] == 'Primary Phone':
						primary_phone = field['@Value'];
					elif field['@FieldTitle'] == 'Credit Score':
						credit_score = field['@Value'];
					elif field['@FieldTitle'] == 'Custom 1':
						custom_1 = field['@Value'];
					elif field['@FieldTitle'] == 'Adressee':
						adressee = field['@Value'];
					elif field['@FieldTitle'] == 'Total System Size (Echo Watts)':
						total_system_size_echowatts = field['@Value'];
					elif field['@FieldTitle'] == 'Total Retail Price Quoted':
						total_retail_price_quoted = field['@Value'];
					elif field['@FieldTitle'] == 'Contract Cancelled Date':
						contract_cancelled_date = field['@Value'];
					elif field['@FieldTitle'] == 'Interested in Purchase Type':
						interested_in_purchase_type = field['@Value'];
					elif field['@FieldTitle'] == 'Financing Program':
						financing_program = field['@Value'];
					elif field['@FieldTitle'] == 'Solar Designer':
						solar_designer = field['@Value'];
					elif field['@FieldTitle'] == 'Lost Reason':
						lost_reason = field['@Value'];
					elif field['@FieldTitle'] == 'Lost':
						Lost = field['@Value'];
					elif field['@FieldTitle'] == 'Lost/Cancel Comments':
						lost_cancel_comments = field['@Value'];
					elif field['@FieldTitle'] == 'Homeowner Lease Contract Status':
						homeowner_lease_contract_status = field['@Value'];
					elif field['@FieldTitle'] == 'Homeowner Credit Check Status':
						homeowner_credit_check_status = field['@Value'];
					elif field['@FieldTitle'] == 'Proposal Status':
						proposal_status = field['@Value'];
					elif field['@FieldTitle'] == 'Campaign SubCategory':
						campaign_subcategory = field['@Value'];
					elif field['@FieldTitle'] == 'Name':
						name = field['@Value'];
					elif field['@FieldTitle'] == 'Address':
						address = field['@Value'];
					elif field['@FieldTitle'] == 'Google Street View Link1':
						google_street_view_link1 = field['@Value'];
					elif field['@FieldTitle'] == 'Email1':
						email1 = field['@Value'];
					elif field['@FieldTitle'] == 'Primary Phone1':
						primary_phone1 = field['@Value'];
					elif field['@FieldTitle'] == 'Home Phone1':
						home_phone1 = field['@Value'];
					elif field['@FieldTitle'] == 'Mobile Phone1':
						mobile_phone1 = field['@Value'];
					elif field['@FieldTitle'] == 'Work Phone1':
						work_phone1 = field['@Value'];
					elif field['@FieldTitle'] == 'International Phone':
						international_phone = field['@Value'];
					elif field['@FieldTitle'] == 'Netsuite Token ID':
						netsuite_token_id = field['@Value'];
					elif field['@FieldTitle'] == 'Advisor Notes':
						advisor_notes = field['@Value'];
					elif field['@FieldTitle'] == 'Solar Advisor':
						solar_advisor = field['@Value'];
					elif field['@FieldTitle'] == 'kW Sold':
						kw_sold = field['@Value'];
					elif field['@FieldTitle'] == 'System Price':
						system_price = field['@Value'];
					elif field['@FieldTitle'] == 'Energy Consultant':
						energy_consultant = field['@Value'];
					elif field['@FieldTitle'] == 'Energy Consultant Notes':
						energy_consultant_notes = field['@Value'];
					elif field['@FieldTitle'] == 'Other Notes':
						other_notes = field['@Value'];
					elif field['@FieldTitle'] == 'Lead Source Manual':
						lead_source_manual = field['@Value'];
					elif field['@FieldTitle'] == 'Designer Notes':
						designer_notes = field['@Value'];
					elif field['@FieldTitle'] == 'Pricing Exceptions':
						pricing_exceptions = field['@Value'];
					elif field['@FieldTitle'] == 'Velocify Lead ID':
						id = field['@Value'];
					elif field['@FieldTitle'] == 'Collected Utility Bill':
						collected_utility_bill = field['@Value'];
					elif field['@FieldTitle'] == 'Contract Status':
						contract_status = field['@Value'];

				createdate = lead['@CreateDate'];
				modifydate = lead['@ModifyDate'];
				last_distribution_date = lead['@LastDistributionDate'];
				lead_title = lead['@LeadTitle'];
				status = lead['Status']['@StatusTitle'];
				if 'Agent' in lead:
					agent_name = lead['Agent']['@AgentName'];
					agent_email = lead['Agent']['@AgentEmail'];
				
				campaign = lead['Campaign']['@CampaignTitle'];

				flag = checkForExisitingIdLeads(id)
				if flag == 0:
					insertLead(firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign)					

		else:
			for field in output["Leads"]["Lead"]["Fields"]["Field"]:
				if field['@FieldTitle'] == 'First Name':
					firstname = field['@Value'];
				elif field['@FieldTitle'] == 'Last Name':
					lastname = field['@Value'];
				elif field['@FieldTitle'] == 'Home Address':
					home_address = field['@Value'];
				elif field['@FieldTitle'] == 'City':
					city = field['@Value'];
				elif field['@FieldTitle'] == 'State':
					state = field['@Value'];
				elif field['@FieldTitle'] == 'Zip':
					zip = field['@Value'];
				elif field['@FieldTitle'] == 'Home Phone':
					home_phone = field['@Value'];
				elif field['@FieldTitle'] == 'Work Phone':
					work_phone = field['@Value'];
				elif field['@FieldTitle'] == 'Email':
					email = field['@Value'];
				elif field['@FieldTitle'] == 'Average Monthly Electric Bill':
					average_monthly_electric_bill = field['@Value'];
				elif field['@FieldTitle'] == 'Electricity Utility Company':
					electricity_utility_company = field['@Value'];
				elif field['@FieldTitle'] == 'Do you own the home?':
					do_you_own_the_home = field['@Value'];
				elif field['@FieldTitle'] == 'Roof Shade':
					roof_shade = field['@Value'];
				elif field['@FieldTitle'] == 'Roof Type':
					roof_type = field['@Value'];
				elif field['@FieldTitle'] == 'Google Street View Link':
					google_street_view_link = field['@Value'];
				elif field['@FieldTitle'] == 'SMS Phone':
					sms_phone = field['@Value'];
				elif field['@FieldTitle'] == 'SMS Opt-Out':
					sms_opt_out = field['@Value'];
				elif field['@FieldTitle'] == 'Netsuite ID':
					netsuite_id = field['@Value'];
				elif field['@FieldTitle'] == 'Lead Source':
					lead_source = field['@Value'];
				elif field['@FieldTitle'] == 'Lead Price Paid ($)':
					lead_price_paid = field['@Value'];
				elif field['@FieldTitle'] == 'Country':
					country = field['@Value'];
				elif field['@FieldTitle'] == 'Duplicate Result from Netsuite':
					duplicate_result_from_netsuite = field['@Value'];
				elif field['@FieldTitle'] == 'Home Type':
					home_type = field['@Value'];
				elif field['@FieldTitle'] == 'Preferred Method of Follow-Up':
					preferred_method_of_follow_up = field['@Value'];
				elif field['@FieldTitle'] == 'Lead Generator Comment':
					lead_generator_comment = field['@Value'];
				elif field['@FieldTitle'] == 'Lead Generator Date':
					lead_generator_date = field['@Value'];
				elif field['@FieldTitle'] == 'Lead Generator Time':
					lead_generator_time = field['@Value'];
				elif field['@FieldTitle'] == 'Yes, I would like to receive updates about SunEdis':
					updates_about_sunedison = field['@Value'];
				elif field['@FieldTitle'] == 'How much do you want to cut your bill?':
					how_much_do_you_want_to_cut_your_bill = field['@Value'];
				elif field['@FieldTitle'] == 'Homeowner require financing?':
					homeowner_require_financing = field['@Value'];
				elif field['@FieldTitle'] == 'Water Heater Type':
					water_heater_type = field['@Value'];
				elif field['@FieldTitle'] == 'Interested In':
					interested_in = field['@Value'];
				elif field['@FieldTitle'] == 'Originator':
					originator = field['@Value'];
				elif field['@FieldTitle'] == 'Originator Email':
					originator_email = field['@Value'];
				elif field['@FieldTitle'] == 'SunEdison Employee Referrer First Name':
					sunedison_employee_referrer_firstname = field['@Value'];
				elif field['@FieldTitle'] == 'SunEdison Employee Referrer Last Name':
					sunedison_employee_referrer_lastname = field['@Value'];
				elif field['@FieldTitle'] == 'SunEdison Employee Referrer Email':
					sunedison_employee_referrer_email = field['@Value'];
				elif field['@FieldTitle'] == 'Does your house have an attic?':
					does_your_house_have_an_attic = field['@Value'];
				elif field['@FieldTitle'] == 'Do you have a central heating and cooling system?':
					do_you_have_a_central_heating_and_cooling_system = field['@Value'];
				elif field['@FieldTitle'] == 'Primary Phone':
					primary_phone = field['@Value'];
				elif field['@FieldTitle'] == 'Credit Score':
					credit_score = field['@Value'];
				elif field['@FieldTitle'] == 'Custom 1':
					custom_1 = field['@Value'];
				elif field['@FieldTitle'] == 'Adressee':
					adressee = field['@Value'];
				elif field['@FieldTitle'] == 'Total System Size (Echo Watts)':
					total_system_size_echowatts = field['@Value'];
				elif field['@FieldTitle'] == 'Total Retail Price Quoted':
					total_retail_price_quoted = field['@Value'];
				elif field['@FieldTitle'] == 'Contract Cancelled Date':
					contract_cancelled_date = field['@Value'];
				elif field['@FieldTitle'] == 'Interested in Purchase Type':
					interested_in_purchase_type = field['@Value'];
				elif field['@FieldTitle'] == 'Financing Program':
					financing_program = field['@Value'];
				elif field['@FieldTitle'] == 'Solar Designer':
					solar_designer = field['@Value'];
				elif field['@FieldTitle'] == 'Lost Reason':
					lost_reason = field['@Value'];
				elif field['@FieldTitle'] == 'Lost':
					Lost = field['@Value'];
				elif field['@FieldTitle'] == 'Lost/Cancel Comments':
					lost_cancel_comments = field['@Value'];
				elif field['@FieldTitle'] == 'Homeowner Lease Contract Status':
					homeowner_lease_contract_status = field['@Value'];
				elif field['@FieldTitle'] == 'Homeowner Credit Check Status':
					homeowner_credit_check_status = field['@Value'];
				elif field['@FieldTitle'] == 'Proposal Status':
					proposal_status = field['@Value'];
				elif field['@FieldTitle'] == 'Campaign SubCategory':
					campaign_subcategory = field['@Value'];
				elif field['@FieldTitle'] == 'Name':
					name = field['@Value'];
				elif field['@FieldTitle'] == 'Address':
					address = field['@Value'];
				elif field['@FieldTitle'] == 'Google Street View Link1':
					google_street_view_link1 = field['@Value'];
				elif field['@FieldTitle'] == 'Email1':
					email1 = field['@Value'];
				elif field['@FieldTitle'] == 'Primary Phone1':
					primary_phone1 = field['@Value'];
				elif field['@FieldTitle'] == 'Home Phone1':
					home_phone1 = field['@Value'];
				elif field['@FieldTitle'] == 'Mobile Phone1':
					mobile_phone1 = field['@Value'];
				elif field['@FieldTitle'] == 'Work Phone1':
					work_phone1 = field['@Value'];
				elif field['@FieldTitle'] == 'International Phone':
					international_phone = field['@Value'];
				elif field['@FieldTitle'] == 'Netsuite Token ID':
					netsuite_token_id = field['@Value'];
				elif field['@FieldTitle'] == 'Advisor Notes':
					advisor_notes = field['@Value'];
				elif field['@FieldTitle'] == 'Solar Advisor':
					solar_advisor = field['@Value'];
				elif field['@FieldTitle'] == 'kW Sold':
					kw_sold = field['@Value'];
				elif field['@FieldTitle'] == 'System Price':
					system_price = field['@Value'];
				elif field['@FieldTitle'] == 'Energy Consultant':
					energy_consultant = field['@Value'];
				elif field['@FieldTitle'] == 'Energy Consultant Notes':
					energy_consultant_notes = field['@Value'];
				elif field['@FieldTitle'] == 'Other Notes':
					other_notes = field['@Value'];
				elif field['@FieldTitle'] == 'Lead Source Manual':
					lead_source_manual = field['@Value'];
				elif field['@FieldTitle'] == 'Designer Notes':
					designer_notes = field['@Value'];
				elif field['@FieldTitle'] == 'Pricing Exceptions':
					pricing_exceptions = field['@Value'];
				elif field['@FieldTitle'] == 'Velocify Lead ID':
					id = field['@Value'];
				elif field['@FieldTitle'] == 'Collected Utility Bill':
					collected_utility_bill = field['@Value'];
				elif field['@FieldTitle'] == 'Contract Status':
					contract_status = field['@Value'];

			createdate = output["Leads"]["Lead"]["@CreateDate"];
			modifydate = output["Leads"]["Lead"]["@ModifyDate"];
			last_distribution_date = output["Leads"]["Lead"]["@LastDistributionDate"];
			lead_title = output["Leads"]["Lead"]["@LeadTitle"];
			status = output["Leads"]["Lead"]["Status"]["@StatusTitle"];
			if 'Agent' in output["Leads"]["Lead"]:
				agent_name = output["Leads"]["Lead"]["Agent"]["@AgentName"];
				agent_email = output["Leads"]["Lead"]["Agent"]["@AgentEmail"];
			campaign = output["Leads"]["Lead"]["Campaign"]["@CampaignTitle"];

			flag = checkForExisitingIdLeads(id)
			if flag == 0:
					insertLead(firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign)					


def getModifiedActionLogs(output):
	AgentName = "";
	ActionTypeName = "";
	ActionNote = "";
	GroupName = "";
	AgentEmail = "";
	ActionDate = "";

	if isinstance(output["Leads"]["Lead"], list):
		for lead in output['Leads']['Lead']:
			if 'ActionLog' in lead['Logs']:
				if isinstance(lead['Logs']['ActionLog']['Action'], list):
					for action in lead['Logs']['ActionLog']['Action']:
						if '@AgentName' in action:
							AgentName = action["@AgentName"];

						if '@ActionTypeName' in action:
							ActionTypeName = action["@ActionTypeName"];

						if '@ActionNote' in action:
							ActionNote = action["@ActionNote"];

						if '@GroupName' in action:
							GroupName = action["@GroupName"];

						if '@AgentEmail' in action:
							AgentEmail = action["@AgentEmail"];

						if '@ActionDate' in action:
							ActionDate = action["@ActionDate"];
						id = lead["@Id"];
						CreateDate = lead["@CreateDate"];
						ModifyDate = lead["@ModifyDate"];

						flag = checkForExisitingIdActionLog(id, ActionDate)
						if flag == 0:
							insertActionLog(AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,id,CreateDate,ModifyDate)
							
				else:
					if '@AgentName' in lead["Logs"]["ActionLog"]["Action"]:
						AgentName = lead["Logs"]["ActionLog"]["Action"]["@AgentName"];

					if '@ActionTypeName' in lead["Logs"]["ActionLog"]["Action"]:
						ActionTypeName = lead["Logs"]["ActionLog"]["Action"]["@ActionTypeName"];

					if '@ActionNote' in lead["Logs"]["ActionLog"]["Action"]:
						ActionNote = lead["Logs"]["ActionLog"]["Action"]["@ActionNote"];

					if '@GroupName' in lead["Logs"]["ActionLog"]["Action"]:
						GroupName = lead["Logs"]["ActionLog"]["Action"]["@GroupName"];

					if '@AgentEmail' in lead["Logs"]["ActionLog"]["Action"]:
						AgentEmail = lead["Logs"]["ActionLog"]["Action"]["@AgentEmail"];

					if '@ActionDate' in lead["Logs"]["ActionLog"]["Action"]:
						ActionDate = lead["Logs"]["ActionLog"]["Action"]["@ActionDate"];		

					id = lead["@Id"];
					CreateDate = lead["@CreateDate"];
					ModifyDate = lead["@ModifyDate"];

					flag = checkForExisitingIdActionLog(id, ActionDate)
					if flag == 0:
							insertActionLog(AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,id,CreateDate,ModifyDate)


def checkForExisitingIdLeads(id):
	cursor = conn.cursor();
	cursor.execute("SELECT * FROM leads_dev where velocify_lead_id='%s'" %id);
	rows = cursor.fetchall();
	if len(rows) > 0:
		flag = 1;
	else:
		flag = 0;
	return flag;

def checkForExisitingIdActionLog(id, actiondate):
	cursor = conn.cursor();
	cursor.execute("SELECT * FROM actionlog_dev where velocify_lead_id='%s'" %id + " and actiondate='%s'" %actiondate);
	rows = cursor.fetchall();
	if len(rows) > 0:
		flag = 1;
	else:
		flag = 0;
	return flag;

def updateActionLog(AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,id,CreateDate,ModifyDate):
	cursor = conn.cursor();
	query = "UPDATE actionlog_dev SET agentname = '%s" %AgentName + "', actiontypename = '%s" %ActionTypeName + "', actionnote = '%s" %ActionNote.replace("'","''") + "', groupname = '%s" %GroupName + "', agentemail = '%s" %AgentEmail + "', actiondate = '%s" %ActionDate + "', velocify_lead_id = '%s" %id + "', createdate = '%s" %CreateDate + "', modifydate = '%s" %ModifyDate + "' WHERE velocify_lead_id =%s" %id + " and actiondate='%s'" %ActionDate;
	try:
		cursor.execute(query)
	except: 
		traceback.print_exc(file=sys.stdout)
		print query;
	conn.commit()
	print "Row updated for ID(ActionLog): %s" %id + " and actiondate: %s" %ActionDate

def insertActionLog(AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,id,CreateDate,ModifyDate):
	cursor = conn.cursor();
	query = "insert into actionlog_dev (agentname,actiontypename,actionnote,groupname,agentemail,actiondate,velocify_lead_id,createdate,modifydate) values ('%s'"%AgentName + ",'%s'"%ActionTypeName + ",'%s'"%ActionNote.replace("'","''") + ",'%s'"%GroupName + ",'%s'"%AgentEmail + ",'%s'"%ActionDate + ",'%s'"%id + ",'%s'"%CreateDate + ",'%s'"%ModifyDate + ")";
	try:
		cursor.execute(query)
	except: 
		traceback.print_exc(file=sys.stdout)
		print query;
	conn.commit()
	print "Row inserted for ID(ActionLog): %s" %id + " and actiondate: %s" %ActionDate

def updateLead(firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign):
	cursor = conn.cursor();
	query = "UPDATE leads_dev SET firstname = '%s" %firstname.replace("'","''") + "', lastname = '%s" %lastname.replace("'","''") + "', home_address = '%s" %home_address.replace("'","''") + "', city = '%s" %city.replace("'","''") + "', state = '%s" %state + "', zip = '%s" %zip + "', home_phone = '%s" %home_phone + "', work_phone = '%s" %work_phone + "', email = '%s" %email + "', average_monthly_electric_bill = '%s" %average_monthly_electric_bill + "', electricity_utility_company = '%s" %electricity_utility_company.replace("'","''") + "', do_you_own_the_home = '%s" %do_you_own_the_home + "', roof_shade = '%s" %roof_shade + "', roof_type = '%s" %roof_type + "', google_street_view_link = '%s" %google_street_view_link + "', sms_phone = '%s" %sms_phone + "', sms_opt_out = '%s" %sms_opt_out + "', netsuite_id = '%s" %netsuite_id + "', lead_source = '%s" %lead_source + "', lead_price_paid_$ = '%s" %lead_price_paid + "', country = '%s" %country + "', duplicate_result_from_netsuite = '%s" %duplicate_result_from_netsuite + "', home_type = '%s" %home_type + "', preferred_method_of_follow_up = '%s" %preferred_method_of_follow_up + "', lead_generator_comment = '%s" %lead_generator_comment.replace("'","''") + "', lead_generator_date = '%s" %lead_generator_date + "', lead_generator_time = '%s" %lead_generator_time + "', updates_about_sunedison = '%s" %updates_about_sunedison + "', how_much_do_you_want_to_cut_your_bill = '%s" %how_much_do_you_want_to_cut_your_bill + "', homeowner_require_financing = '%s" %homeowner_require_financing + "', water_heater_type = '%s" %water_heater_type + "', interested_in = '%s" %interested_in + "', originator = '%s" %originator + "', originator_email = '%s" %originator_email + "', sunedison_employee_referrer_firstname = '%s" %sunedison_employee_referrer_firstname + "', sunedison_employee_referrer_lastname = '%s" %sunedison_employee_referrer_lastname + "', sunedison_employee_referrer_email = '%s" %sunedison_employee_referrer_email + "', does_your_house_have_an_attic = '%s" %does_your_house_have_an_attic + "', do_you_have_a_central_heating_and_cooling_system = '%s" %do_you_have_a_central_heating_and_cooling_system + "', primary_phone = '%s" %primary_phone + "', credit_score = '%s" %credit_score + "', custom_1 = '%s" %custom_1 + "', adressee = '%s" %adressee + "', total_system_size_echowatts = '%s" %total_system_size_echowatts + "', total_retail_price_quoted = '%s" %total_retail_price_quoted + "', contract_cancelled_date = '%s" %contract_cancelled_date + "', interested_in_purchase_type = '%s" %interested_in_purchase_type + "', financing_program = '%s" %financing_program + "', solar_designer = '%s" %solar_designer + "', lost_reason = '%s" %lost_reason + "', lost = '%s" %lost + "', lost_cancel_comments = '%s" %lost_cancel_comments.replace("'","''") + "', homeowner_lease_contract_status = '%s" %homeowner_lease_contract_status + "', homeowner_credit_check_status = '%s" %homeowner_credit_check_status + "', address_2 = '%s" %address_2 + "', proposal_status = '%s" %proposal_status + "', campaign_subcategory = '%s" %campaign_subcategory + "', name = '%s" %name + "', address = '%s" %address + "', google_street_view_link1 = '%s" %google_street_view_link1 + "', email1 = '%s" %email1 + "', primary_phone1 = '%s" %primary_phone1 + "', home_phone1 = '%s" %home_phone1 + "', mobile_phone1 = '%s" %mobile_phone1 + "', work_phone1 = '%s" %work_phone1 + "', international_phone = '%s" %international_phone + "', netsuite_token_id = '%s" %netsuite_token_id + "', advisor_notes = '%s" %advisor_notes.replace("'","''") + "', solar_advisor = '%s" %solar_advisor + "', kw_sold = '%s" %kw_sold + "', system_price = '%s" %system_price + "', energy_consultant = '%s" %energy_consultant + "', energy_consultant_notes = '%s" %energy_consultant_notes.replace("'","''") + "', other_notes = '%s" %other_notes.replace("'","''") + "', lead_source_manual = '%s" %lead_source_manual + "', designer_notes = '%s" %designer_notes.replace("'","''") + "', pricing_exceptions = '%s" %pricing_exceptions + "', velocify_lead_id = '%s" %id + "', collected_utility_bill = '%s" %collected_utility_bill + "', contract_status = '%s" %contract_status + "', createdate = '%s" %createdate + "', modifydate = '%s" %modifydate + "', last_distribution_date = '%s" %last_distribution_date + "', lead_title = '%s" %lead_title + "', status = '%s" %status + "', agent_name = '%s" %agent_name + "', agent_email = '%s" %agent_email + "', campaign = '%s" %campaign + "' WHERE velocify_lead_id =%s" %id;
	try:
		cursor.execute(query)
	except: 
		traceback.print_exc(file=sys.stdout)
		print query;
	conn.commit()
	print "Row updated for ID(Lead): %s" %id

def insertLead(firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign):
	cursor = conn.cursor();
	query = "insert into leads_dev (firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid_$,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,velocify_lead_id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign) values ('%s'"%firstname.replace("'","''") + ",'%s'"%lastname.replace("'","''") + ",'%s'"%home_address.replace("'","''") + ",'%s'"%city.replace("'","''") + ",'%s'"%state + ",'%s'"%zip + ",'%s'"%home_phone + ",'%s'"%work_phone + ",'%s'"%email + ",'%s'"%average_monthly_electric_bill + ",'%s'"%electricity_utility_company + ",'%s'"%do_you_own_the_home + ",'%s'"%roof_shade + ",'%s'"%roof_type + ",'%s'"%google_street_view_link + ",'%s'"%sms_phone + ",'%s'"%sms_opt_out + ",'%s'"%netsuite_id + ",'%s'"%lead_source + ",'%s'"%lead_price_paid + ",'%s'"%country + ",'%s'"%duplicate_result_from_netsuite + ",'%s'"%home_type + ",'%s'"%preferred_method_of_follow_up + ",'%s'"%lead_generator_comment.replace("'","''") + ",'%s'"%lead_generator_date + ",'%s'"%lead_generator_time + ",'%s'"%updates_about_sunedison + ",'%s'"%how_much_do_you_want_to_cut_your_bill + ",'%s'"%homeowner_require_financing + ",'%s'"%water_heater_type + ",'%s'"%interested_in + ",'%s'"%originator + ",'%s'"%originator_email + ",'%s'"%sunedison_employee_referrer_firstname + ",'%s'"%sunedison_employee_referrer_lastname + ",'%s'"%sunedison_employee_referrer_email + ",'%s'"%does_your_house_have_an_attic + ",'%s'"%do_you_have_a_central_heating_and_cooling_system + ",'%s'"%primary_phone + ",'%s'"%credit_score + ",'%s'"%custom_1 + ",'%s'"%adressee + ",'%s'"%total_system_size_echowatts + ",'%s'"%total_retail_price_quoted + ",'%s'"%contract_cancelled_date + ",'%s'"%interested_in_purchase_type + ",'%s'"%financing_program + ",'%s'"%solar_designer + ",'%s'"%lost_reason + ",'%s'"%lost + ",'%s'"%lost_cancel_comments.replace("'","''") + ",'%s'"%homeowner_lease_contract_status + ",'%s'"%homeowner_credit_check_status + ",'%s'"%address_2 + ",'%s'"%proposal_status + ",'%s'"%campaign_subcategory + ",'%s'"%name + ",'%s'"%address + ",'%s'"%google_street_view_link1 + ",'%s'"%email1 + ",'%s'"%primary_phone1 + ",'%s'"%home_phone1 + ",'%s'"%mobile_phone1 + ",'%s'"%work_phone1 + ",'%s'"%international_phone + ",'%s'"%netsuite_token_id + ",'%s'"%advisor_notes.replace("'","''") + ",'%s'"%solar_advisor + ",'%s'"%kw_sold + ",'%s'"%system_price + ",'%s'"%energy_consultant + ",'%s'"%energy_consultant_notes.replace("'","''") + ",'%s'"%other_notes.replace("'","''") + ",'%s'"%lead_source_manual + ",'%s'"%designer_notes.replace("'","''") + ",'%s'"%pricing_exceptions + ",'%s'"%id + ",'%s'"%collected_utility_bill + ",'%s'"%contract_status + ",'%s'"%createdate + ",'%s'"%modifydate + ",'%s'"%last_distribution_date + ",'%s'"%lead_title + ",'%s'"%status + ",'%s'"%agent_name + ",'%s'"%agent_email + ",'%s'"%campaign + ")";
	try:
		cursor.execute(query)
	except: 
		traceback.print_exc(file=sys.stdout)
		print query;
	conn.commit()
	print "Row inserted for ID(Lead): %s" %id

startHere();

