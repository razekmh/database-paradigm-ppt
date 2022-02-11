'''extract the emails and their attributes from the enron dataset'''

# import modules
import os
from email import message_from_string
import json

def list_folders(parent_folder):
    # list the folders in the parent folder
    onlyfolder = [os.path.join(parent_folder, f) for f in os.listdir(parent_folder) if not os.path.isfile(os.path.join(parent_folder, f))]
    return onlyfolder

def list_files(parent_folder):
    # list the files in the parent folder
    onlyfiles = [os.path.join(parent_folder, f) for f in os.listdir(parent_folder) if os.path.isfile(os.path.join(parent_folder, f))]
    return onlyfiles

def clean_email_attr(email_attr):
    # clean the email attributes

    cleaned_email = email_attr
    return cleaned_email


def extract_email_data(main_folder, file_path, sub_folder=None):
    # extract the email data from the file
    with open(file_path, encoding = "ISO-8859-1") as f:
            print("file read: " + file)
            # read the file
            content = f.read()
            # parse the email data
            parsed_mail = message_from_string(content)
            # extract the email message id
            email_message_id = parsed_mail['Message-ID']
            # extract the email date
            email_date = parsed_mail['Date']
            # extract the email from
            email_from = parsed_mail['From']
            # extract the email to
            email_to = parsed_mail['To']
            # extract the email subject
            email_subject = parsed_mail['Subject']
            # extract the email cc
            email_cc = parsed_mail['Cc']
            # extract the email bcc
            email_bcc = parsed_mail['Bcc']
            # extract the email body
            email_body = parsed_mail.get_payload().replace('"','\\"')



            # create a dictionary to store the email attributes
            email_dict = {}
            # add the email attributes to the dictionary
            email_dict['email_message_id'] = email_message_id
            email_dict['email_date'] = email_date
            email_dict['email_from'] = email_from
            email_dict['email_to'] = email_to
            email_dict['email_subject'] = email_subject
            email_dict['email_cc'] = email_cc
            email_dict['email_bcc'] = email_bcc
            email_dict['email_body'] = email_body
            email_dict['main_folder'] = main_folder
            email_dict['sub_folder'] = sub_folder
            
            # write the dictionary to the output file
            with open(out_file, 'a', encoding="utf-8") as outfile:
                json.dump(email_dict, outfile)
                outfile.write('\n')

# detect current file path
path = os.path.dirname(os.path.abspath(__file__))

# define the input and output file paths
in_folder = os.path.join(path, 'data/enron')

# define the path to the output file
out_file = os.path.join(os.path.join(path, 'data'), 'enron_emails.json')

# extract list of folders in the enron dataset
enron_folders = list_folders(in_folder)


for folder in enron_folders:
    # print(f"Processing folder: {folder}")
    # extract list of files in each folder
    enron_files = list_files(folder)
    
    # get main folder name
    main_folder = (folder.split('/')[-1])

    # extract emails in the main folder 
    for file in enron_files:
        extract_email_data(main_folder, file)
    
    # extract emails in the sub folders
    sub_folders = list_folders(folder)

    for sub_folder in sub_folders:
        # print(f"Processing sub folder: {sub_folder}")
        # extract list of files in each sub folder
        sub_files = list_files(sub_folder)
        
        # get sub folder name
        sub_folder = (sub_folder.split('/')[-1])

        # extract emails in the sub folder
        for file in sub_files:
            extract_email_data(main_folder, file, sub_folder)



test_json = {"email_message_id": "<29781703.1075840053264.JavaMail.evans@thyme>", "email_date": "Thu, 24 Jan 2002 13:18:41 -0800 (PST)", "email_from": "aahyman@bpa.gov", "email_to": "e-mail <.al@enron.com>, e-mail <.allan@enron.com>, e-mail <.arley@enron.com>, \n\te-mail <.ben@enron.com>, e-mail <.bert@enron.com>, \n\te-mail <.betta@enron.com>, e-mail <.bill@enron.com>, \n\te-mail <.bill@enron.com>, e-mail <.bob@enron.com>, \n\te-mail <.brenda@enron.com>, e-mail <.brian@enron.com>, \n\te-mail <.bruce@enron.com>, e-mail <.carolyn@enron.com>, \n\te-mail <.carolyn@enron.com>, e-mail <.chris@enron.com>, \n\te-mail <.christy@enron.com>, e-mail <.dan@enron.com>, \n\te-mail <.dan@enron.com>, e-mail <.dan@enron.com>, \n\te-mail <.dave@enron.com>, e-mail <.david@enron.com>, \n\te-mail <.dawn@enron.com>, e-mail <.dick@enron.com>, \n\te-mail <.don@enron.com>, e-mail <.doug@enron.com>, \n\te-mail <.doug@enron.com>, e-mail <.eric@enron.com>, \n\te-mail <.eric@enron.com>, e-mail <.erik@enron.com>, \n\te-mail <.gillian@enron.com>, e-mail <.gina@enron.com>, \n\te-mail <.grant@enron.com>, e-mail <.harriet@enron.com>, \n\te-mail <.harry@enron.com>, e-mail <.jackman@enron.com>, \n\te-mail <.jana@enron.com>, e-mail <.jonathan@enron.com>, \n\te-mail <.jude@enron.com>, e-mail <.kevin@enron.com>, \n\te-mail <.kristian@enron.com>, e-mail <.lance@enron.com>, \n\te-mail <.lance@enron.com>, e-mail <.laurie@enron.com>, \n\te-mail <.ley@enron.com>, e-mail <.lynda@enron.com>, \n\te-mail <.michelle@enron.com>, e-mail <.nan@enron.com>, \n\te-mail <.norm@enron.com>, e-mail <.peter@enron.com>, \n\te-mail <.rob@enron.com>, e-mail <.robert@enron.com>, \n\te-mail <.scott@enron.com>, e-mail <.steve@enron.com>, \n\te-mail <.tom@enron.com>, jenovich.adrian@enron.com, \n\tbrettmann.al@enron.com, gibbs.al@enron.com, srahan.amy@enron.com, \n\tobserver.argus@enron.com, ziesmer.b.@enron.com, \n\tblanton.becky@enron.com, caldwell.bert@enron.com, \n\trudolph.bill@enron.com, virgin.bill@enron.com, ward.bill@enron.com, \n\trice.brad@enron.com, lincoln.brenda@enron.com, \n\tgorman.brian@enron.com, cash.cathy@enron.com, cbs@enron.com, \n\t26.channel@enron.com, huggett.chris@enron.com, perry.clare@enron.com, \n\tperry.clare@enron.com, office.congresswoman@enron.com, \n\troianello.craig@enron.com, rose.craig@enron.com, \n\tmoyer.cynthia@enron.com, noe.cyrus@enron.com, keith.darcy@enron.com, \n\thassler.darrel@enron.com, bogoslaw.david@enron.com, \n\tharris.david@enron.com, taub.david@enron.com, \n\tkinirons.deborah@enron.com, shanon.dennis@enron.com, \n\tbrimhall.diana@enron.com, cross.diana@enron.com, \n\tmeier.dutch@enron.com, boling.ed@enron.com, maurina.ed@enron.com, \n\twhite.ed@enron.com, today.energy@enron.com, mowrer.erica@enron.com, \n\tkqqq <.evan@enron.com>, hill.gail@enron.com, \n\toconnor.gillian@enron.com, palmer.griffin@enron.com, \n\thelwig.heidi@enron.com, kenneth.hemmelman@enron.com, \n\thrnews@enron.com, broome.j.@enron.com, opb.jeff@enron.com, \n\tcastellano.jeff@enron.com, senior.jenny@enron.com, \n\tpowell.jessica@enron.com, pazzanghera.jim@enron.com, \n\tjewett.joan@enron.com, harrison.john@enron.com, \n\tstucke.john@enron.com, woolfolk.john@enron.com, \n\tbrinkman.johnathan@enron.com, err.johnathan@enron.com, \n\tvorman.julie@enron.com, seinfeld.keith@enron.com, \n\thettler.kelly@enron.com, eber.kevin@enron.com, kgw@enron.com, \n\tkoin@enron.com, koin@enron.com, kregg.arntson@enron.com, \n\tvasquez.krist@enron.com, mcnicholas.kym@enron.com, \n\tdickie.lance@enron.com, hennessay.laurie@enron.com, \n\tgarnett.lee@enron.com, broska.m.@enron.com, barton.mary@enron.com, \n\trabe.matt@enron.com, bulletin.metals@enron.com, \n\tgoodbody.michael@enron.com, cole.michelle@enron.com, \n\tkpnd <.mike@enron.com>, gudgell.mike@enron.com, lee.mike@enron.com, \n\to'bryant.mike@enron.com, wilczek.mike@enron.com, \n\thunt.nigel@enron.com, conway.nola@enron.com, \n\tcommunity.northwest@enron.com, news.nugget@enron.com, \n\talleva.p.@enron.com, harrison.pat@enron.com, mcman.pat@enron.com, \n\thowe.patti@enron.com, touradji.paul@enron.com, \n\trichardson.peter@enron.com, stavros.r.@enron.com, \n\tsmith.rebecca@enron.com, ilgenfritz.ric@enron.com, \n\tgavin.robert@enron.com, gibbons.robert@enron.com, \n\tstokes.robert@enron.com, harnack.roger@enron.com, \n\twire.romel@enron.com, george.russ@enron.com, george.russ@enron.com, \n\tmiller.scott@enron.com, sean.crandall@enron.com, \n\tcronin.sean@enron.com, swartz.spencer@enron.com, \n\tsprings.spilyay@enron.com, heiser.steve@enron.com, \n\tjones.steve@enron.com, gordon.susan@enron.com, dale.sylvie@enron.com, \n\tbaer.theresa@enron.com, shinabarger.tim@enron.com, \n\tnpr <.tom@enron.com>, detzel2.tom@enron.com, conrad.trish@enron.com, \n\tphinney.w.@enron.com, willy@enron.com, review.yakama@enron.com, \n\therald.yakima@enron.com, howard.zach@enron.com", "email_subject": "Bonneville Power Administration News Release", "email_cc": null, "email_bcc": null, "email_body": "\n <<...OLE_Obj...>> \n\n\n\nNEWS MEDIA CONTACTS:\t\t\t\tFOR IMMEDIATE RELEASE\nJill Schroeder 202/586-4940\t\t\t\t\tThursday,\nJanuary 24, 2002\nTom Welch 202/586-5806\t\n\nEnergy Secretary Names Bonneville Head\n\nWASHINGTON, DC -- Department of Energy Secretary Spencer Abraham announced\ntoday the appointment of Steve Wright as administrator of the Bonneville\nPower Administration (BPA).  BPA is a federal agency that markets wholesale\nelectrical power, principally from federal dams in the Columbia Basin, and\noperates high voltage transmission for the region.  It is based in Portland,\nOregon, and serves Washington, Idaho, Oregon, western Montana and small\nparts of contiguous states.\n\n\\\"Bonneville is extremely important to maintaining a vital economy and\nhealthy environment in the Pacific Northwest, and I have confidence it will\ncontinue to do so under Steve Wright's direction,\\\" said Secretary Abraham.\n\\\"Steve exerted outstanding leadership through some of the most turbulent\ntimes for the electricity industry and has effectively worked with diverse\nconstituencies in the region,\\\" added Abraham.\n\nWright has been with the agency for 21 years, beginning in the agency's\nconservation department.  He has served as acting administrator since\nNovember 2000 following the departure of then-Administrator Judi Johansen.\nWright's tenure has spanned some of the region's biggest energy\nchallenges--including a West Coast energy crisis and the second worst\ndrought recorded in Pacific Northwest history.  \n\nWright said he was particularly honored to have come through the ranks to\nhave the opportunity to lead an institution that is critical to meeting the\neconomic and environmental objectives of Northwest citizens.  \\\"I'm committed\nto operating BPA as a sound business enterprise, but one whose mission\nstarts with meeting our responsibilities to serve the public.  That means\nstriving for strong environmental stewardship, low rates, reliable service\nand being open to the public's ideas about how best to use the assets that\nprovide so much value to the Northwest,\\\" Wright said.\n\nWright began his work at BPA in 1981 shortly after receiving his masters in\npublic affairs from the University of Oregon.  Following a stint in the\nagency's energy conservation office, he moved to BPA's D.C. office in 1984.\nIn 1987, he began managing the agency's California office, then went back to\nthe D.C. office as manager in 1990.  In 1998, he returned to BPA\nheadquarters in Portland as Corporate senior vice president where he had\nresponsibility for public affairs; environment, fish and wildlife; finance\nand budget; strategic planning; and human resources.  He became deputy\nadministrator in 2000, followed shortly by his appointment as acting\nadministrator.  \n\nHe and his wife, Kathleen, and their three children live in Portland.  In\naddition to his  masters, he holds an undergraduate bachelor of arts degree\nin journalism from Central Michigan University.  He is 44 years old.\n-DOE-  \nR-02-009\t\n\nA portrait photo is available at:   ftp://ftp.bpa.gov/outgoing/steve_wright.\n", "main_folder": "crandell-s", "sub_folder": "inbox"}
