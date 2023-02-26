
#!/usr/bin/perl
use strict;
use warnings;
use Time::Local;
use Net::SMTP;

my $input_day = shift;

my $CATReporterIMID = "DART";
my $CATSubmitterID = "140802";
##SUMO CRD 146310
##GS CRD 361

my %senderimids = (
    140 => '140802:DEGS',
    517 => '140802:DEGS',
    56  => '140802:DEGS',
    369 => '140802:DEGS',
    469 => '140802:DEGS',
    569 => '140802:DEGS',
    123 => '140802:DEGS',
    158 => '140802:DEGS',
    629 => '140802:DEGS',
    146 => '140802:DEGS',
	188 => '140802:DEGS',
	190 => '140802:DEGS'
);

#3NE4F|X3NE3|3NE31209|3NE41209|3NEF909|31XJ1209
my %badmods = (
    '3NE31209' => '149224:GTSB',
    '3NE41209' => '149224:GTSB',
    '3NE3F909' => '149224:GTSB',
    '31XJ1209' => '146310:SUMO'
);
my%clientimids = (
    '3NE31209' => '149224:GTSB',
    '3NE41209' => '149224:GTSB',
    '3NE3F909' => '149224:GTSB',
    '40000321' => '161014:EDFP',
    '40000320' => '161014:EDFP'
);

my %custAccounts = (
    'AR161209'=>'AR161209',
);

my %exchid = (
	140=>'ARCA',
    517=>'ARCA',
    146=>'NSDQ',
    629=>'7897:INCA',
    56=>'361:GSCS',
    158=>'149823:NITE',
    123=>'116797:CDRG',
    369=>'116797:CDRG',
    469=>'116797:CDRG',
    569=>'116797:CDRG',
	188=>'EDGXOP',
	190=>'NYSE');
	
my %otcexchid = (
    "GBTC"=>'116797:CDEL',
    "ADYEY"=>'116797:CDEL'
);

my %desttypes = (
    140=>'E',
    517=>'E',
    146=>'E',
    629=>'F',
    56=> 'F',
    123=>'F',
    369=>'F',
    469=>'F',
    569=>'F',
    158=>'F',	
    188=>'E',
	190=>'E');


my %sessionids = (
	140=>'PDART07',
    517=>'PDART07',
    146=>'DEGSR1',
    188=>'DART0001',
	190=>'NFDEGS01');

my $iscentral = 0;
my %reptimes;
my %outs;
my %orig_times;
my %rejects;
my %replacerej;
my $sequence = 60650;
my $file_sequence = 15;
my $file_h = &set_file($CATSubmitterID);


# Variables for 2d
my $CATactionType = "NEW";
my $CATerrorROEID = "";
my @CATtype = ("MENO","MEOA","MEOR","MEOC","MEOM","MONO","MOOA","MOOR","MOOC","MOOM","MLOR","MLNO","MLOA","MLOM","MLOC");
my $CATmanualFlag="false";
my $CATelectronicDupFlag="false";
my $CATelectronicTimestamp="";
my $CATmanualOrderKeyDate="";
my $CATmanualOrderID="";
my $CATsolicitationFlag="false";
my $CATRFQID = "";
my $CATtradingSession="REG";
my $CATcustDspIntrFlag="false";
my $CATinfoBarierID="";
my $CATaggregatedOrders="";
my $CATnegotiatedTradeFlag=""; ### should be boolean!!!!
my $CATrepresentativeInd="N";
my $CATseqNum = "";
my $CATatsField="";
my $CATreceiverIMID=$CATSubmitterID.":".$CATReporterIMID;
my $CATisoInd="NA";
my $CATpairedOrderID = "";
my $CAToriginatingIMID="";
my $CATmultiLegInd="false";
my $CATquoteKeyDate="";
my $CATquoteID="";
my $CATpriorOrderKeyDate="";
my $CATpriorOrderID="";
my $CATreserved="";
my $CATrouteRejectedFlag="false";
my $CATretiredFieldPosition="";
my $CATdupROIDCond="false";

# temporary variables for 2d
my $CATnetPrice="";
my @CATdeptType = ("O","A","T");
my $CATminQty="";
my @CATaccountHolerType = ("O","A","P");
my @CATaffiliateFlag = ("TRUE","FALSE");
my $CATleavesQty=0;
my $CATinitiator="C";
my $CATrequestTimestamp="";
my @CATsenderType = ("E","O","F"); #line 112
my @CAThandlingInstructions=("DIR","RAR","ALG","PEG","");


my $tzinfo = `strings /etc/localtime | egrep -o "[CE]ST[56][CE]DT"`;
if (!defined($tzinfo) || length($tzinfo) == 0) {
    print "cannot determine time zone\n";
}
if ($tzinfo =~ "CST6CDT") {
    $iscentral = 1;
    print "Central time\n";
} else {
    print "Eastern time\n";
}
print "$tzinfo\n";


my @files = <BOLT*equity.txt>; #119
my @sfiles =  sort {
    ($a =~ /_(\d+)/)[0] <=> ($b =~ /_(\d+)/)[0]
} @files;
foreach my $file (@sfiles) {
    open(IN, "<$file") or die "cannot open $file\n";
    while(<IN>) {
        chomp;
        my @lroms = split(/,/);
        if($lroms[0] eq "S"){
            if( $lroms[14] eq "13") {
                $outs{$lroms[17]} = $lroms[15];
            }
            if( $lroms[14] eq "27") {
                $reptimes{$lroms[15]} = &create_time_str($lroms[52]);
            }
            if($lroms[14] eq "8") {
                $rejects{$lroms[17]} = "true";
                $outs{$lroms[17]} = $lroms[15];
            }
	    if($lroms[14] eq "20" || $lroms[14] eq "30") {
                $replacerej{$lroms[15]} = "true";
	    }
        } elsif ($lroms[0] eq "E") {
            $orig_times{$lroms[17]} = &create_time_str($lroms[52]);
        }
    }
    close(IN);
}
foreach my $file (@sfiles) {
    open(IN, "<$file") or die "cannot open $file\n";

    while (<IN>) {
        chomp;
        my @romfields = split(/,/);
        #if ($romfields[59] =~ m/^S /){
        #	my @eq_leg = split(/ /, $romfields[59]);
        #	$romfields[4] = $eq_leg[1];
        #	$romfields[6] = $eq_leg[2];
        #}
        my $sym = $romfields[5];
        if($sym eq "ZVZZT") {
            print "Test order: $sym, $romfields[41], $romfields[13], $romfields[17] \n";
        } else {
            my $imid = $senderimids{$romfields[13]};
            if(defined $imid) {
                if($romfields[0] eq "E" ) {
                    my $custAccount = $custAccounts{$romfields[12]};
                    if(defined $custAccount) {
                       &create_new_order(\@romfields);
                    } else {
                        &create_order_accpted(\@romfields);
                    }
                    &create_order_routed(\@romfields);
                }
                if($romfields[14] eq "26") {
                    &create_order_cancel(\@romfields);
                }
                if($romfields[14] eq "5") {
                    &create_order_modify(\@romfields);
                }
            } else {
                if($romfields[41] ne "369") {
                    print "Not in the Senderimid Map: $romfields[13],romtag $romfields[17] \n";
                }
            }
        }
    }
    close(IN);
}


#
#
#
#
#  MENO.Row("NEW", None, createRoeID(csv.Exchange, csv.Romtime),
#       "MENO",Some imi.imid, convertTime csv, csv.Clienttag,
#        csv.Symbol, utcToEastern csv.Romtime, "false", "false", None, None,
#        None,custAccount.DeptType, None, None,
#        translateSide csv.Side, csv.Price, csv.Shares,
#        csv.Minqty, convertOrderType csv.Ordertype,
#        convertTif( csv.Tif, utcToEastern(csv.Romtime)), getSession csv.Tif,
#        Some (buildHandlingInstructions csv), (getDisplay(csv.Maxfloor)),
#       firmid, holder, custAccount.Affiliate.ToString(),
#       None, None, "false", "N", None, None,None, None, None,
#       None, None, None, None, None,None,None))


sub create_new_order {
    my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                         #1
        $CATerrorROEID,                                         #2
        &create_fore_id($lroms->[17], $lroms->[52]),            #3
        $CATtype[0],                                            #4
        $CATReporterIMID,                                       #5
        &create_time_str($lroms->[52]),                         #6 orderKeyDate
        $lroms->[17],                                           #7 orderID
        $lroms->[5],                                            #8 symbol
        &create_time_str($lroms->[52]),                         #9 eventTimestamp
        $CATmanualFlag,                                         #10 
        $CATelectronicDupFlag,                                  #11
        $CATelectronicTimestamp,                                #12
        $CATmanualOrderKeyDate,                                 #13
        $CATmanualOrderID,                                      #14
        $CATdeptType[2],                                        #15 &&get_dept_type
        $CATsolicitationFlag,                                   #16
        $CATRFQID,                                              #17
        &convert_side($lroms->[4]),                             #18
        &checkPrice($lroms->[7], $lroms->[8]),                  #19
        $lroms->[6],                                            #20 quantity
        $CATminQty,                                             #21 &&get_minqty
        &convert_type($lroms->[8]),                             #22 orderType
        &checkTif($lroms->[9], $lroms->[52]),                   #23
        $CATtradingSession,                                     #24
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[0]),   #25
        $CATcustDspIntrFlag,                                    #26
        $lroms->[12],                                           #27
        $CATaccountHolerType[1],                                #28 &&get_account_holder_type
        $CATaffiliateFlag[0],                                   #29 &&get_affiliation
        $CATinfoBarierID,                                       #30
        $CATaggregatedOrders,                                   #31
        $CATnegotiatedTradeFlag,                                #32
        $CATrepresentativeInd,                                  #33
        $CATatsField,                                           #34
        $CATatsField,                                           #35
        $CATatsField,                                           #36
        $CATatsField,                                           #37
        $CATatsField,                                           #38
        $CATatsField,                                           #39
        $CATatsField,                                           #40
        $CATatsField,                                           #41
        $CATatsField,                                           #42
        $CATatsField,                                           #43
        $CATatsField,                                           #44
        $CATatsField,                                           #45
        $CATnetPrice                                            #46 &&get_net_price
    );
    my $lf = $file_h->{"file"};
    print $lf $output;

}
#
#
#
#

sub create_order_accpted {
    my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                     #1
        $CATerrorROEID,                                     #2
        &create_fore_id($lroms->[17], $lroms->[52]),        #3
        $CATtype[1],                                        #4
        $CATReporterIMID,                                   #5
        &create_time_str($lroms->[52]),                     #6
        $lroms->[17],                                       #7
        &clean_sym($lroms->[5]),                            #8
        &create_time_str($lroms->[52]),                     #9
        $CATmanualFlag,                                     #10
        $CATelectronicDupFlag,                              #11
        $CATelectronicTimestamp,                            #12
        $CATreceiverIMID,                                   #13
        &get_sender_imid_for_clrid($lroms->[12]),           #14
        $CATsenderType[2],                                  #15 &&get_sender_type
        $lroms->[3],                                        #16
        $CATmanualOrderKeyDate,                             #17
        $CATmanualOrderID,                                  #18
        $CATaffiliateFlag[0],                               #19 &&get_affiliation
        $CATdeptType[1],                                    #20 &&get_dept_type
        &convert_side($lroms->[4]),                         #21
        &checkPrice($lroms->[7], $lroms->[8]),              #22
        $lroms->[6],                                        #23
        $CATminQty,                                         #24 &&get_minqty
        &convert_type($lroms->[8]),                         #25
        &checkTif($lroms->[9], $lroms->[52]),               #26
        &checkSessions($lroms->[13]),                        #27
        $CATisoInd,                                         #28
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[1]),                        #29 &&handlingInstruction->&setHandlingInstructions
        $CATcustDspIntrFlag,                                #30
        $CATinfoBarierID,                                   #31
        $CATatsField,                                       #32
        $CATatsField,                                       #33
        $CATatsField,                                       #34
        $CATatsField,                                       #35
        $CATatsField,                                       #36
        $CATatsField,                                       #37
        $CATatsField,                                       #38
        $CATatsField,                                       #39
        $CATatsField,                                       #40
        $CATatsField,                                       #41
        $CATatsField,                                       #42
        $CATatsField,                                       #43
        $CATsolicitationFlag,                               #44
        $CATpairedOrderID,                                  #45
        $CATnetPrice                                        #46 &&get_net_price
    );                                                      #line 326
    my $lf = $file_h->{"file"};
    print $lf $output;

}

#
#
#
#

sub create_order_routed {
    my $lroms = shift;
    my $output = sprintf ("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                     #1
        $CATerrorROEID,                                     #2
        &create_fore_id($lroms->[17], $lroms->[52]),        #3
        $CATtype[2],                                        #4
        $CATReporterIMID,                                   #5
        &create_time_str($lroms->[52]),                     #6
        $lroms->[17],                                       #7
        &clean_sym($lroms->[5]),                            #8
        $CAToriginatingIMID,                                #9
        &create_time_str($lroms->[52]),                     #10
        $CATmanualFlag,                                     #11
        $CATelectronicDupFlag,                              #12
        $CATelectronicTimestamp,                            #13
        &get_sender_imid_for_dest($lroms->[13]),            #14
        &get_imid_for_dest($lroms->[13],$lroms->[5]),                   #15
        &get_dest_type($lroms->[13]),                       #16
        &getRoutedID($lroms->[17], $lroms->[3]),            #17
        &getSessionID($lroms->[13]),                        #18
        &convert_side($lroms->[4]),                         #19
        &checkPrice($lroms->[7], $lroms->[8]),              #20
        $lroms->[6],                                        #21
        $CATminQty,                                         #22 &&get_minqty
        &convert_type($lroms->[8]),                         #23
        &checkTif($lroms->[9],$lroms->[52]),                #24
        &checkSessions($lroms->[13]),                       #25
        $CATaffiliateFlag[1],                               #26 &&get_affiliation
        $CATisoInd,                                         #27
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[2]),   #28 &handlingInstruction->&setHandlingInstructions
        &checkReject($lroms->[17]),                               #29 #need to fix!!!
        $CATdupROIDCond,                         #30 false
        $CATseqNum,                                         #31
        $CATmultiLegInd,                                    #32
        $CATpairedOrderID,                                  #33
        $CATinfoBarierID,                                   #34
        $CATnetPrice,                                       #35
        $CATquoteKeyDate,                                   #36
        $CATquoteID                                         #37
    );
    my $lf = $file_h->{"file"};
    print $lf $output;
}


#
#

sub create_order_modify {
    my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                                           #1
        $CATerrorROEID,                                                           #2
        &create_fore_id($lroms->[17], $lroms->[52]),                              #3
        $CATtype[4],                                                              #4
        $CATReporterIMID,                                                         #5
        &getOriginalTime($lroms->[17], $lroms->[52]),                             #6
        $lroms->[17],                                                             #7
        &clean_sym($lroms->[5]),                                                  #8
        $CATpriorOrderKeyDate,                                                    #9
        $CATpriorOrderID,                                                         #10
        $CAToriginatingIMID,                                                      #11
        &getModifyTime($lroms->[59], $lroms->[52]),                               #12
        $CATmanualFlag,                                                           #13
        $CATmanualOrderKeyDate,                                                   #14
        $CATmanualOrderID,                                                        #15
        $CATelectronicDupFlag,                                                    #16
        $CATelectronicTimestamp,                                                  #17
        $CATreceiverIMID,                                                         #18
        &get_sender_imid_for_clrid($lroms->[12]),                                 #19
        $CATsenderType[2],                                                        #20 &&get_sender_type
        &get_routed_id_for_modify($lroms->[59],$lroms->[3], $lroms->[28]),        #21
        $CATrequestTimestamp,                                                     #22
        $CATreserved,                                                          #23
        $CATreserved,                                                          #24
        $CATreserved,                                                          #25
        $CATinitiator,                                                            #26
        &convert_side($lroms->[4]),                                               #27
        &checkPrice($lroms->[7], $lroms->[8]),                                    #28
        $lroms->[6],                                                              #29
        $CATminQty,                                                               #30 &&get_minqty
        $lroms->[49],                                                             #31
        &convert_type($lroms->[8]),                                               #32
        &checkTif($lroms->[9], $lroms->[52]),                                     #33
        &checkSessions($lroms->[13]),                                             #34
        $CATisoInd,                                                               #35
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[4]),        #36 &handlingInstruction->&setHandlingInstructions
        $CATcustDspIntrFlag,                                                      #37
        $CATinfoBarierID,                                                         #38
        $CATaggregatedOrders,                                                     #39
        $CATrepresentativeInd,                                                    #40
        $CATseqNum,                                                               #41
        $CATatsField,                                                             #42
        $CATatsField,                                                             #43
        $CATatsField,                                                             #44
        $CATatsField,                                                             #45
        $CATatsField,                                                             #46
        $CATatsField,                                                             #47
        $CATatsField,                                                             #48
        $CATatsField,                                                             #49
        $CATatsField,                                                             #50
        $CATatsField,                                                             #51
        $CATatsField,                                                             #52
        $CATnetPrice                                                              #53 &&get_net_price
    );
    my $lf = $file_h->{"file"};
    print $lf $output;
    my $output2 = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                     #1
        $CATerrorROEID,                                     #2
        &create_fore_id($lroms->[17], $lroms->[52]),        #3
        $CATtype[2],                                        #4
        $CATReporterIMID,                                   #5
        &getOriginalTime($lroms->[17], $lroms->[52]),         #6
		$lroms->[17],                                       #7
        &clean_sym($lroms->[5]),                            #8
        $CAToriginatingIMID,                                #9
        &getModifyTime($lroms->[59], $lroms->[52]),         #10
		$CATmanualFlag,                                     #11
        $CATelectronicDupFlag,                              #12
        $CATelectronicTimestamp,                            #13
        &get_sender_imid_for_dest($lroms->[13]),            #14
        &get_imid_for_dest($lroms->[13],$lroms->[5]),                   #15
        &get_dest_type($lroms->[13]),                       #16
        $lroms->[15],            #17
        &getSessionID($lroms->[13]),                        #18
        &convert_side($lroms->[4]),                         #19
        &checkPrice($lroms->[7], $lroms->[8]),              #20
        $lroms->[6],                                        #21
        $CATminQty,                                         #22 &&get_minqty
        &convert_type($lroms->[8]),                         #23
        &checkTif($lroms->[9],$lroms->[52]),                #24
        &checkSessions($lroms->[13]),                       #25
        $CATaffiliateFlag[1],                               #26 &&get_affiliation
        $CATisoInd,                                         #27
        &setHandlingInstructions($lroms->[57], $lroms->[73], $CATtype[2]),  #28 &handlingInstruction->&setHandlingInstructions
		&checkRepRej($lroms->[15]),                           #29
        $CATdupROIDCond,                              #30
        $CATseqNum,                                         #31
        $CATmultiLegInd,                                    #32
        $CATpairedOrderID,                                  #33
        $CATinfoBarierID,                                   #34
        $CATnetPrice,                                       #35
        $CATquoteKeyDate,                                   #36
        $CATquoteID                                         #37
    );
    print $lf $output2;
}



sub create_order_cancel {
    my $lroms = shift;
    my $output = sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        $CATactionType,                                             #1
        $CATerrorROEID,                                             #2
        &create_fore_id($lroms->[17], $lroms->[52]),                #3
        $CATtype[3],                                                #4
        $CATReporterIMID,                                           #5
        &getOriginalTime($lroms->[17], $lroms->[52]),               #6
        $lroms->[17],                                               #7
        &clean_sym($lroms->[5]),                                    #8
        $CAToriginatingIMID,                                        #9
        &create_time_str($lroms->[52]),                             #10
        $CATmanualFlag,                                             #11
        $CATelectronicTimestamp,                                    #12
        &determine_cancelled_qty($lroms->[6],$lroms->[48]),         #13
        $CATleavesQty,                                              #14 &&get_leave_qty;
        $CATinitiator,                                              #15
        $CATseqNum,                                                 #16
        $CATrequestTimestamp,                                       #17 &&get_request_time
        $CATinfoBarierID                                            #18
    );
    my $lf = $file_h->{"file"};
    print $lf $output;
}


sub getModifyTime {
    my $myLastID = shift;
    my $myDefRomTime = shift;
    my $time = $reptimes{$myLastID};
    if(defined $time) {
        $time;
    } else {
        print "Could not find Rep sending time for $myLastID \n";
        &create_time_str($myDefRomTime);
    }
}

sub getOriginalTime {
    my $id = shift;
    my $myDefRomTime = shift;
    my $time = $orig_times{$id};
    if(defined $time) {
        $time;
    } else {
        print "Could not find original sending time for $id \n";
        &create_time_str($myDefRomTime);
    }
}

sub checkReject {
	my $id = shift;
	my $rej = $rejects{$id};
	if(defined $rej) {
		$rej;
	} else {
		"false";
	}
}

sub checkRepRej { #not being used
	my $id = shift;
	my $rej = $replacerej{$id};
	if(defined $rej) {
		$rej;
	} else {
		"false";
	}
}

sub getSessionID {
    my $dest = shift;
    my $exch = $sessionids{$dest};
    if(defined $exch) {
        $exch;
    } else {
        "";
    }
}
sub getOpenClose { #not being used
    my $oc = shift;
    if($oc eq "1") {
        "Open";
    } else {
        "Close";
    }
}
sub determine_cancelled_qty {
    my $size = shift;
    my $cum = shift;
    my $rez = $size - $cum;
    if($rez < 0) {
        $rez=0;
    }
    $rez;
}


# sub create_file {
#     my $who = shift;
#     my $sec;
#     my $min;
#     my $hour;
#     my $mday;
#     my $mon;
#     my $year;
#     my $wday;
#     my $yday;
#     my $isdst;
#     ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
#     sprintf("%s_DART_%04d%02d%02d_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $file_sequence);
# }

sub create_file {
	my $who = shift;
	my $input_day = shift;
	if(defined $input_day){
		sprintf("%s_DART_%d_BOLTEquity_OrderEvents_%06d.csv", $who, $input_day, $file_sequence);
	}else{
		my $sec;
		my $min;
		my $hour;
		my $mday;
		my $mon;
		my $year;
		my $wday;
		my $yday;
		my $isdst;
		($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
		sprintf("%s_DART_%04d%02d%02d_BOLTEquity_OrderEvents_%06d.csv", $who, $year + 1900, $mon + 1, $mday, $file_sequence);
	}
}

sub set_file {
    my $mpid = shift;
	my $file_name = &create_file($mpid, $input_day);
    my $conn = {};
    my $FILEH;
    open($FILEH, ">", $file_name) or die "cannot open $file_name\n";
    $conn->{"file"} = $FILEH;
    $conn->{"rec"} = 0;
    $conn;
}

sub create_header_date { #not being used
    my $sec;
    my $min;
    my $hour;
    my $mday;
    my $mon;
    my $year;
    my $wday;
    my $yday;
    my $isdst;
    ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
    sprintf("%04d%02d%02d", $year + 1900, $mon + 1, $mday);

}

sub create_fore_id {
    my $romtag = shift;
    my $romtime = shift;
    my $localRom = &create_time_str($romtime);
    $sequence += 1;
    sprintf("%s_%s-%d", substr($localRom, 0, 8), $romtag, $sequence);
}

sub convert_side
{
    my $side = shift;
    if(defined $side) {
        if($side eq "1") {
            "B";
        } elsif($side eq "2") {
            "SL";
        } elsif($side eq "5") {
            "SS";
        } elsif($side eq "6") {
            "SX";
        }
    }
}

sub create_tif_day_date {
    my $utcDate = shift;
    my $local = &create_time_str($utcDate);
    substr($local, 0,8);
}

sub create_time_str {
    my $time_str = shift;
    if ($time_str =~ /(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2}).(\d{3})/) {
        my $time = timegm($6, $5, $4, $3, $2 - 1, $1);
        my $milli = $7;
        #print "$milli\n";
        my ($sec, $min, $hour, $mday, $mon, $year) = localtime($time);
        if ($iscentral > 0) {
            $hour += 1;
        }
        sprintf("%04d%02d%02d %02d%02d%02d.%03d",
            $year + 1900, $mon + 1, $mday, $hour, $min, $sec, $milli);
    }
    elsif ($time_str =~ /(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})/) {
        my $time = timegm($6, $5, $4, $3, $2 - 1, $1);
        my ($sec, $min, $hour, $mday, $mon, $year) = localtime($time);
        if ($iscentral > 0) {
            $hour += 1;
        }
        sprintf("%04d%02d%02d %02d%02d%02d.000",
            $year + 1900, $mon + 1, $mday, $hour, $min, $sec);
    }
}

sub get_sender_imid_for_dest {
    my $dest = shift;
    my $imid = $senderimids{$dest};
    if(defined $imid) {
        $imid;
    } else {
        print "Failed to find imid for $dest \n";
        "DEGS";
    }
}

sub get_routed_id_for_modify {
    my $myLastID = shift;
    my $route_id = shift;
    my $om_ex_tag = shift;
    my $time = $reptimes{$myLastID};
    if(defined $time) {
		if(length($route_id) < 5) {
			$om_ex_tag;
		} else {
			$route_id;
		}
   } else {
		$om_ex_tag
   }
}

# sub get_routed_id_for_modify {
#     my $clr_acc = shift;
#     my $route_id = shift;
#     my $om_ex_tag = shift;
#     my $imid = $badmods{$clr_acc};
#     if(defined $imid) {
#         $om_ex_tag;
#     } else {
#         $route_id;
#     }
# }

sub get_sender_imid_for_clrid {
    my $clr_acc = shift;
    my $imid = $clientimids{$clr_acc};
    if(defined $imid) {
        $imid;
    } else {
        "146310:SUMZ";
    }
}

sub get_imid_for_dest {
    my $dest = shift;
    my $symbol = shift;
    my $exch = $exchid{$dest};
    my $otcexch = $otcexchid{$symbol};
    if($dest eq "369" and defined $otcexch){
    	$otcexch
    }elsif(defined $exch) {
        $exch;
    } else {
        print "Failed to find exchange id for $dest \n";
        "";
    }
}

sub get_dest_type {
    my $dest = shift;
    my $dtype =  $desttypes{$dest};
    if(defined $dtype) {
        $dtype;
    } else {
        print "Failed to find desttype from $dest \n";
        "E";
    }
}

sub getSymbol { #not being used
    my $sym = shift;
    $sym =~ s/_/ /g;
    $sym;
}

sub checkPrice {
    my $price = shift;
    my $type = shift;
    if(($type eq "1") or ($type eq "0")) {
        "";
    } else {
    	my $decimal = index($price, ".")+1;
	 	$price = substr($price,0,$decimal).substr($price,$decimal,8);
        #$price;
    }
}

sub checkTif {
    my $tif = shift;
    my $time = shift;
    if($tif eq "3") {
        "IOC";
    } else {
        my $rtif = "DAY=" . &create_tif_day_date($time);
        $rtif;
    }
}

sub checkSessions {
    my $dest = shift;
    if($dest eq "517") {
        "ALL";
    } else {
        "REG";
    }
}

sub convert_type {
    my $type = shift;
    if(($type eq "1") or ($type eq "P") or ($type eq 0) or ($type eq 3) or ($type eq 5) or ($type eq 12)) {
        "MKT";
    } else {
        "LMT";
    }
}

sub getRoutedID {
    my $refid = shift;
    my $backup = shift;
    my $orig = $outs{$refid};
    if(defined $orig and length($orig) > 0) {
        $orig;
    } else {
        $backup;
    }
}
sub clean_sym
{
    my $sym = shift;
    if(defined $sym) {
        $sym =~ s/\// /g;
        $sym =~ s/\./ /g;
    }
    $sym;
}

# 57 Execution Instruction, 73 AlgoType E message

sub setHandlingInstructions{
    my $type = shift;
    my $algoFlag = shift;
    my $event = shift;
    if($type eq "1" and $event eq "MEOA"){
    	"DIR|NH"
    }elsif($event eq "MENO" or $event eq "MEOA" or $event eq "MEOM") {
        if(defined $algoFlag and $algoFlag ne "0") {
            "DIR|ALG";
        } elsif(defined $type) {
            if($type eq "P" or $type eq "M" or $type eq "R") {
            "DIR|PEG";
            } else {
        		"DIR";
            }
        } else {
            "DIR";
        }
    } elsif($event eq "MEOR") {
		"RAR";
	} else {
		""}
}

#### modification note
# 2/4/2022:
# add MEOA to get DIR instruction in sub setHandlingInstructions
# 2/7/2022
# change deptType for MENO and MEOA from "O" to "A"
# add DIR for event MEOM
# add DIR to MEOM event.
# 4/4/2022 add "-" between the romtag and the sequence number in sub create_fore_id
# 4/8/2022 new exchange 188 and 190 in the hashmaps
# 4/12/2022 changed the desitnation from EDGX to EDGXOP for 188.
# 20220506: add an otcexchid map for the symbols traded over counter and modified get_imid_for_dest sub.
# 20220526: add     "ADYEY"=>'116797:CDEL'
# 20220608: add "DIR|NH" instruction and change "DIR|RAR" to "DIR" in sub setHandlingInstructions
# 20220902: added 140 in the hashmaps where 517 is and with the same value as 517 has. It will roll out to production on 9/6 morning.