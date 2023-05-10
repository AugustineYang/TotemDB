package drz.tmdb.map;

import android.net.wifi.aware.PublishDiscoverySession;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import drz.tmdb.Transaction.Transactions.CreateDeputyClass;
import drz.tmdb.Transaction.Transactions.CreateTJoinDeputyClass;
import drz.tmdb.Transaction.Transactions.impl.CreateDeputyClassImpl;
import drz.tmdb.Transaction.Transactions.impl.CreateTJoinDeputyClassImpl;
import drz.tmdb.memory.Tuple;
import drz.tmdb.Transaction.Transactions.Create;
import drz.tmdb.Transaction.Transactions.Insert;
import drz.tmdb.Transaction.Transactions.Select;
import drz.tmdb.Transaction.Transactions.impl.CreateImpl;
import drz.tmdb.Transaction.Transactions.impl.InsertImpl;
import drz.tmdb.Transaction.Transactions.impl.SelectImpl;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

public class TrajectoryUtils {

    private static MemConnect memConnect;

    public TrajectoryUtils(MemConnect memConnect){
        this.memConnect = memConnect;
        // 初始化两张轨迹表
        init();
    }

    // 初始化两张轨迹表
    public void init(){
        try{
            String sql1 = "CREATE CLASS IF NOT EXISTS mobile_phone_traj (trajectory_id int,user_id char, trajectory char);";
            String sql2 = "CREATE CLASS IF NOT EXISTS watch_traj (trajectory_id int,user_id char, trajectory char);";
            String sql3 = "CREATE TJOINDEPUTYCLASS join_traj as select * from mobile_phone_traj, watch_traj";
            Create create = new CreateImpl(memConnect);
            CreateTJoinDeputyClass createtjoin = new CreateTJoinDeputyClassImpl(memConnect);
            Statement parse1 = CCJSqlParserUtil.parse(sql1);
            Statement parse2 = CCJSqlParserUtil.parse(sql2);
            Statement parse3 = CCJSqlParserUtil.parse(sql3);
            create.create(parse1);
            create.create(parse2);
            createtjoin.createTJoinDeputyClass(parse3);
        }catch (Exception e){
            e.printStackTrace();
        }
        // debug setup data
//        try{
//            Statement ins1 = CCJSqlParserUtil.parse("INSERT INTO mobile_phone_traj VALUES(1, \"uid\",'30.539568-114.3589-30.539568-114.3589-30.539568-114.3589-30.539568-114.3589-30.539568-114.3589-30.54003939-114.3585809-30.54006086-114.3586057-30.54005919-114.3586062-30.54003334-114.3586424-30.54004499-114.3587022-30.54005532-114.3586981-30.54007052-114.3586988-30.54009413-114.3586843-30.5400928-114.35867-30.54009131-114.3586796-30.54009924-114.3586711-30.54010854-114.3586677-30.54011725-114.3586687-30.54012372-114.3586718-30.54013097-114.3586763-30.54013714-114.3586804-30.54014316-114.3586846-30.54015066-114.3586875-30.54015779-114.3586895-30.54016533-114.3586903-30.54017236-114.3586895-30.54017998-114.3586874-30.54018898-114.358685-30.54019668-114.3586835-30.54020443-114.358681-30.54021443-114.3586785-30.54022438-114.3586759-30.54023462-114.3586745-30.54024593-114.3586738-30.54025736-114.3586743-30.54026717-114.358676-30.54027641-114.3586802-30.5402832-114.3586863-30.54029071-114.358696-30.54029724-114.3587098-30.54030241-114.3587245-30.54030715-114.3587403-30.54031323-114.358757-30.54031863-114.3587732-30.54032514-114.358788-30.5403331-114.3588025-30.54033871-114.3588187-30.54034702-114.3588342-30.54035608-114.3588523-30.54036343-114.3588668-30.54037363-114.3588811-30.54038268-114.3588968-30.5403877-114.3589146-30.54039084-114.3589323-30.54039333-114.3589479-30.54039921-114.358964-30.54040489-114.3589781-30.54040909-114.3589923-30.54041647-114.3590071-30.54042805-114.3590203-30.54044071-114.3590315-30.54045572-114.3590401-30.54046895-114.3590502-30.54048043-114.3590613-30.54048823-114.3590738-30.54049396-114.3590867-30.54049836-114.3591022-30.54050116-114.3591194-30.54050477-114.3591369-30.54050739-114.3591542-30.5405091-114.3591716-30.54051253-114.3591877-30.54051498-114.3592034-30.5405168-114.3592201-30.54051805-114.3592385-30.54052231-114.3592536-30.54052846-114.3592657-30.54053611-114.3592759-30.54054399-114.3592848-30.54055253-114.3592938-30.54056125-114.3593042-30.54056857-114.3593165-30.54057262-114.3593286-30.54057679-114.3593407-30.5405828-114.3593521-30.54058984-114.3593625-30.54059788-114.3593715-30.54060606-114.3593809-30.54061362-114.3593905-30.54061817-114.359398-30.54061827-114.359403-30.54061936-114.3594091-30.54062118-114.3594195-30.54062475-114.3594326-30.54062932-114.3594446-30.5406329-114.3594559-30.54063584-114.3594661-30.54063824-114.3594767-30.54063984-114.3594874-30.54064202-114.3594974-30.5406453-114.3595072-30.54064932-114.3595174-30.54065253-114.3595284-30.54065589-114.3595398-30.54065733-114.3595534-30.54066173-114.3595666-30.54066927-114.35958-30.54067811-114.3595922-30.54068971-114.3596005-30.54070428-114.3596038-30.54072002-114.3596036-30.54073584-114.359602-30.54075094-114.3596015-30.54076546-114.3596009-30.54078002-114.3595987-30.5407932-114.3595951-30.54080641-114.3595914-30.54081974-114.3595867-30.54083406-114.3595811-30.54084895-114.3595733-30.5408627-114.3595651-30.54087622-114.3595566-30.54089023-114.3595487-30.54090404-114.3595405-30.54091732-114.3595326-30.54093155-114.3595245-30.54094655-114.3595172-30.54096069-114.3595105-30.54097508-114.3595056-30.5409896-114.3595026-30.54100409-114.3594998-30.54101756-114.3594968-30.54103167-114.3594917-30.54104584-114.3594854-30.54108199-114.3594762-30.54108898-114.3594738-30.54110832-114.3594516-30.54112096-114.359448-30.54112627-114.3594452-30.54113966-114.3594384-30.541149-114.3594337-30.54116099-114.3594293-30.54117243-114.359425-30.54118426-114.3594207-30.54119295-114.359419-30.54119612-114.3594172-30.54119715-114.3594168-30.54119895-114.3594164-30.54120446-114.3594168-30.54121232-114.3594163-30.54122092-114.3594152-30.5412296-114.3594148-30.54123836-114.359416-30.54124934-114.3594188-30.54125821-114.3594234-30.54126544-114.3594298-30.54127122-114.3594373-30.54127659-114.3594458-30.541282-114.3594549-30.54128612-114.3594648-30.54129133-114.3594768-30.54129783-114.3594876-30.54130786-114.3594956-30.54131766-114.3595044-30.54132648-114.3595128-30.54133332-114.3595232-30.54133972-114.3595351-30.54134615-114.3595476-30.5413526-114.35956-30.54135879-114.3595715-30.54136511-114.359584-30.54137112-114.3595961-30.54137664-114.3596085-30.54138125-114.3596221-30.54138589-114.3596352-30.54139019-114.359649-30.54139633-114.3596619-30.54140411-114.359674-30.5414125-114.3596855-30.54142131-114.3596972-30.54143017-114.3597094-30.54143884-114.3597223-30.54144748-114.3597358-30.54145674-114.3597496-30.54146566-114.3597621-30.5414742-114.3597763-30.54148086-114.3597904-30.54148641-114.3598056-30.54149145-114.3598206-30.54149732-114.3598366-30.54150418-114.3598523-30.54151163-114.3598661-30.5415204-114.3598792-30.5415306-114.3598928-30.54153982-114.3599076-30.54154791-114.3599216-30.54155513-114.3599359-30.54156069-114.3599507-30.54156569-114.359965-30.54157113-114.3599782-30.54157783-114.3599906-30.54158505-114.3600033-30.54159173-114.360017-30.54159682-114.3600322-30.54160106-114.3600464-30.54160551-114.360064-30.54160905-114.3600812-30.54161193-114.3600967-30.54161059-114.3601132-30.54161134-114.3601312-30.54160955-114.3601487-30.54160392-114.3601646-30.54159562-114.3601809-30.54158468-114.3601945-30.54156985-114.3602054-30.54155312-114.3602147-30.54153519-114.360219-30.54151769-114.3602232-30.54150003-114.3602276-30.54148209-114.3602333-30.54146308-114.360235-30.54144434-114.3602358-30.54142623-114.3602422-30.54140813-114.3602482-30.54139139-114.3602546-30.54137403-114.360259-30.54135835-114.3602595-30.54134616-114.3602615-30.54133262-114.3602601-30.54131918-114.3602583-30.54130618-114.3602604-30.54128943-114.360256-30.5412743-114.3602581-30.54126001-114.3602619-30.54123954-114.3602532-30.54122329-114.3602601-30.54120712-114.3602603-30.54119164-114.3602608-30.54118215-114.3602601-30.54117291-114.3602587-30.54115808-114.3602598-30.54114357-114.3602626-30.54112843-114.3602674-30.54111295-114.360274-30.54109984-114.36028-30.54108602-114.360287-30.54107115-114.3602921-30.54105507-114.3602971-30.54103793-114.3603036-30.54101814-114.360312-30.54099971-114.3603205-30.54098168-114.3603306-30.54096351-114.3603406-30.54094431-114.3603504-30.54092626-114.360359-30.54090833-114.3603665-30.54089123-114.3603737-30.54087322-114.3603806-30.54085537-114.3603868-30.54083741-114.3603907-30.54081881-114.3603925-30.54079971-114.3603951-30.54078173-114.3603968-30.54076516-114.3603973-30.54074845-114.3603992-30.54073275-114.3604025-30.54071915-114.3604067-30.54070544-114.3604122-30.54069141-114.3604169-30.54067522-114.3604195-30.54065915-114.3604211-30.54064613-114.3604258-30.54063413-114.3604319-30.54062051-114.3604381-30.54060364-114.3604428-30.5405836-114.3604479-30.54056496-114.3604542-30.54054675-114.3604626-30.54052896-114.3604701-30.54051333-114.360478-30.54049872-114.3604873-30.54048798-114.3604974-30.54047751-114.3605079-30.54046782-114.3605186-30.54045648-114.3605284-30.54044241-114.3605393-30.54042959-114.3605562-30.54041827-114.3605769-30.54041216-114.3606032-30.54040843-114.3606286-30.54040514-114.3606516-30.54040233-114.360673-30.54039783-114.3606911-30.54039266-114.3607094-30.54039127-114.3607254-30.540394-114.3607406-30.54040002-114.3607554-30.54040783-114.3607735-30.54041405-114.3607914-30.5404175-114.3608101-30.54041703-114.3608275-30.54041595-114.3608451-30.54041247-114.3608616-30.54040883-114.360876-30.5404077-114.3608903-30.54040712-114.3609047-30.54040608-114.3609242-30.54040659-114.3609424-30.54040772-114.3609602-30.54041375-114.3609762-30.54042328-114.3609904-30.54043388-114.361004-30.54044446-114.3610158-30.5404539-114.3610293-30.54046116-114.3610434-30.54046842-114.3610575-30.54047611-114.3610699-30.54048746-114.3610812-30.54050206-114.3610862-30.5405185-114.3610891-30.54053608-114.3610942-30.54055211-114.3611018-30.54056689-114.3611128-30.54058172-114.3611236-30.54059761-114.3611336-30.54061327-114.3611428-30.54062919-114.3611516-30.54064317-114.3611623-30.54065562-114.361173-30.54066916-114.3611807-30.54068335-114.3611851-30.54069907-114.361188-30.54071475-114.3611873-30.54073176-114.3611844-30.54074743-114.3611786-30.54076306-114.3611746-30.54077994-114.3611703-30.54079588-114.3611686-30.54081179-114.3611678-30.54082615-114.3611676-30.54083994-114.3611698-30.54085648-114.3611711-30.54087429-114.3611733-30.5408931-114.3611741-30.54091017-114.3611759-30.54092599-114.3611783-30.54094168-114.3611799-30.5409555-114.3611806-30.54096712-114.3611803-30.5409771-114.3611783-30.54098748-114.3611745-30.54099926-114.361169-30.541013-114.3611642-30.54102634-114.3611612-30.54103913-114.3611598-30.54105151-114.3611582-30.54106463-114.361155-30.54107705-114.3611508-30.54109201-114.3611474-30.54110554-114.3611455-30.54112071-114.3611438-30.54113546-114.3611421-30.54115167-114.361141-30.54116814-114.3611408-30.54118509-114.3611402-30.54120312-114.3611386-30.54122022-114.3611361-30.54123758-114.3611333-30.54125466-114.3611309-30.54127177-114.3611284-30.54128881-114.3611258-30.54130603-114.361123-30.54132226-114.3611207-30.54133765-114.3611196-30.54135462-114.3611185-30.54137081-114.361118-30.54138468-114.3611179-30.54139843-114.3611173-30.54141357-114.361116-30.54142825-114.3611142-30.54144294-114.3611112-30.54145676-114.3611085-30.54147213-114.3611058-30.541487-114.3611039-30.54150346-114.3611014-30.54152232-114.3610997-30.54154144-114.3611004-30.5415581-114.3611024-30.54157368-114.3611022-30.54158823-114.3610989-30.54160325-114.3610913-30.54161706-114.3610814-30.54163223-114.3610702-30.54164924-114.3610594-30.54166596-114.3610499-30.54168211-114.3610418-30.54169552-114.361034-30.54170895-114.3610245-30.54172185-114.3610133-30.54173407-114.3610005-30.54174473-114.3609868-30.5417542-114.3609731-30.5417641-114.3609593-30.54177457-114.3609456-30.54178408-114.3609311-30.54179186-114.3609154-30.54179758-114.360899-30.54180278-114.3608821-30.54180815-114.3608642-30.54181278-114.3608468-30.54181774-114.360829-30.54182105-114.3608117-30.54182389-114.3607942-30.54182431-114.3607768-30.54182371-114.3607592-30.54182296-114.3607412-30.54182263-114.3607238-30.54182259-114.3607073-30.54182345-114.36069-30.54182497-114.3606711-30.5418277-114.3606525-30.54182963-114.3606338-30.54182958-114.3606144-30.54182742-114.3605957-30.54182329-114.360576-30.54181879-114.3605556-30.54181431-114.3605329-30.54180941-114.3605104-30.5418037-114.3604893-30.54179597-114.3604674-30.54178594-114.3604467-30.541774-114.3604289-30.54176339-114.3604129-30.54175387-114.3603946-30.54174516-114.3603739-30.54173516-114.3603544-30.5417237-114.3603366-30.54171127-114.3603193-30.54170031-114.3603034-30.54169053-114.3602896-30.54168145-114.3602753-30.54167303-114.3602592-30.54166558-114.360244-30.54165751-114.3602302-30.54164808-114.3602169-30.54164073-114.3602032-30.5416346-114.3601896-30.54163045-114.3601736-30.54162555-114.360158-30.5416197-114.3601423-30.54161456-114.3601255-30.54161069-114.3601076-30.54160703-114.3600899-30.54160346-114.3600709-30.54160015-114.3600525-30.54159838-114.3600344-30.5415946-114.3600161-30.54158749-114.3600013-30.54157811-114.3599919-30.54156621-114.3599857-30.54155267-114.359979-30.54153705-114.3599757-30.54152234-114.3599761-30.54150995-114.3599778-30.54149575-114.3599759-30.54148159-114.3599733-30.54146824-114.3599733-30.54145129-114.3599746-30.54143694-114.3599776-30.54142448-114.359979-30.5414104-114.3599818-30.54139726-114.3599823-30.54138405-114.3599813-30.54136964-114.359979-30.54135462-114.3599773-30.54133876-114.359975-30.54132531-114.3599741-30.54131171-114.3599712-30.54129792-114.359968-30.5412843-114.3599649-30.54127019-114.3599631-30.54125472-114.3599619-30.54123956-114.359959-30.54122294-114.3599551-30.54120632-114.3599538-30.54118981-114.359954-30.5411743-114.3599528-30.541159-114.3599517-30.54114563-114.3599498-30.54113453-114.3599496-30.54112401-114.3599515-30.5411142-114.3599543-30.54110381-114.3599595-30.5410947-114.3599644-30.54108605-114.3599702-30.54107539-114.3599746-30.54106644-114.3599765-30.54105785-114.3599778-30.54104922-114.3599807-30.54103905-114.3599864-30.54102905-114.3599911-30.54102057-114.3599925-30.5410146-114.359991-30.541006-114.3599899-30.54099407-114.359991-30.54098303-114.3599933-30.54097128-114.3599959-30.54096066-114.3599996-30.54094924-114.360002-30.54093695-114.360005-30.54092515-114.3600088-30.54091342-114.360011-30.5409018-114.3600126-30.54088891-114.3600131-30.54087528-114.3600125-30.54086048-114.3600109-30.54084637-114.3600077-30.54083277-114.360002-30.54081954-114.3599949-30.54080345-114.3599893-30.54078773-114.3599835-30.54077172-114.3599758-30.54075406-114.3599685-30.54073694-114.3599668-30.54071752-114.3599703-30.54069805-114.3599771-30.54067935-114.3599866-30.54065896-114.3599973-30.54063844-114.3600067-30.54061676-114.3600121-30.54059484-114.3600133-30.54057146-114.3600159-30.54054904-114.3600221-30.5405279-114.3600342-30.54050683-114.3600493-30.5404864-114.3600655-30.54046641-114.3600822-30.54044758-114.3600974-30.54043025-114.3601119-30.54041516-114.3601235-30.54039983-114.3601347-30.54038651-114.3601521-30.54037718-114.3601696-30.54036455-114.3601833-30.54035282-114.3601985-30.54033872-114.3602086-30.54032251-114.3602215-30.54030742-114.3602358-30.54029237-114.3602486-30.54027816-114.3602605-30.54026512-114.3602717-30.54025241-114.3602834-30.54023976-114.3602964-30.54022845-114.3603114-30.54021694-114.3603255-30.54020439-114.3603378-30.54019645-114.3603541-30.54018522-114.3603676-30.54017523-114.3603821-30.54016463-114.3603944-30.5401516-114.3604047-30.54013786-114.3604119-30.54012605-114.3604218-30.54011244-114.3604293-30.5400965-114.3604324-30.54007965-114.3604336-30.54006325-114.360427-30.54004972-114.3604122-30.54004057-114.3603928-30.54003187-114.3603757-30.54002577-114.3603572-30.54002008-114.3603375-30.5400148-114.3603181-30.54001164-114.3602969-30.54001169-114.3602785-30.54001073-114.3602608-30.54000873-114.3602396-30.54000767-114.3602168-30.54000632-114.3601944-30.54000734-114.3601752-30.54000851-114.3601581-30.5400114-114.3601345-30.54001487-114.360117-30.54001648-114.3601022-30.54001647-114.3600803-30.54001393-114.3600568-30.5400098-114.3600381-30.54000342-114.3600205-30.53999912-114.3599973-30.53999922-114.3599739-30.54000185-114.3599517-30.54000263-114.3599306-30.54000042-114.3599112-30.53999811-114.3598922-30.5399954-114.3598762-30.53999225-114.3598606-30.5399892-114.3598474-30.53998707-114.3598352-30.539984-114.3598222-30.53998131-114.3598082-30.53997746-114.3597901-30.53997162-114.3597734-30.53996531-114.3597592-30.53995907-114.3597456-30.53995243-114.3597314-30.53994413-114.3597169-30.53993324-114.3597073-30.53992392-114.3596975-30.53991733-114.3596853-30.53991416-114.3596747-30.53991444-114.3596627-30.53991389-114.3596481-30.53991049-114.3596326-30.53990426-114.3596201-30.53989586-114.3596078-30.53988707-114.3595947-30.53987939-114.3595793-30.53987211-114.3595641-30.53986587-114.3595541-30.5398602-114.3595407-30.53985481-114.3595313-30.53984822-114.3595238-30.53984247-114.3595154-30.53983757-114.3595083-30.53983255-114.3594123-30.5397665-114.3594522-30.53975567-114.3594746-30.53974503-114.3594654-30.53973568-114.3594572-30.53972801-114.3594494-30.5397232-114.359443-30.5397169-114.3594333-30.53971128-114.3594222-30.53970697-114.3594136-30.53969765-114.3594012-30.539686-114.3593855-30.53967357-114.3593694-30.53965949-114.3593541-30.53964473-114.3593412-30.53963233-114.359333-30.53961908-114.3593293-30.53960286-114.3593278-30.53958762-114.3593276-30.53957245-114.3593252-30.53955702-114.3593187-30.53954268-114.3593099-30.53953011-114.3593007-30.53951927-114.3592902-30.53950848-114.3592796-30.53949889-114.3592672-30.539492-114.3592533-30.53948474-114.3592392-30.53947641-114.3592248-30.53946722-114.3592127-30.53945565-114.359201-30.53944262-114.3591902-30.53943033-114.3591808-30.53941845-114.3591703-30.53940616-114.3591601-30.53939409-114.3591516-30.53938149-114.3591453-30.53937143-114.3591399');\n");
//            Statement ins2 = CCJSqlParserUtil.parse("INSERT INTO watch_traj VALUES(2, \"uid\",'30.539312-114.359138-30.53974777-114.3592024-30.53971671-114.3591737-30.5396366-114.3591797-30.53959998-114.3591745-30.53956712-114.3591772-30.53955121-114.3591823-30.53953815-114.3591864-30.53952453-114.3591915-30.539511-114.3591893-30.5394993-114.3591825-30.53949225-114.359176-30.53948848-114.359167-30.53948417-114.3591588-30.53947929-114.3591484-30.5394743-114.3591352-30.53946933-114.3591225-30.53946747-114.3591078-30.5394692-114.3590902-30.53947158-114.3590717-30.539312-114.359146-30.53941643-114.3589794-30.53943405-114.3589204-30.53944366-114.3588653-30.53945235-114.3588319-30.53946839-114.3588316-30.53946196-114.3588267-30.53945849-114.3588208-30.53946155-114.3588127-30.53945788-114.3588031-30.53945686-114.3587848-30.53945897-114.3587697-30.53946102-114.3587569-30.53946367-114.3587483-30.53946058-114.3587373-30.53945721-114.3587267-30.5394535-114.3587146-30.53944941-114.3587-30.53944508-114.3586831-30.53944099-114.3586669-30.53943635-114.3586499-30.53943214-114.3586319-30.5394316-114.3586124-30.53943222-114.3585925-30.53943312-114.3585734-30.53943503-114.3585538-30.53943868-114.3585375-30.53944332-114.3585206-30.53945192-114.3585057-30.53946333-114.358494-30.53947519-114.3584848-30.53948538-114.3584774-30.53949543-114.3584757-30.53950549-114.358478-30.53951624-114.3584785-30.53952391-114.3584803-30.53953167-114.3584829-30.53954111-114.3584892-30.53955103-114.3584926-30.53956136-114.3584952-30.53957152-114.3584966-30.53958308-114.3584964-30.53959628-114.3584957-30.5396099-114.3584925-30.5396233-114.3584896-30.53963595-114.3584926-30.53964813-114.3584981-30.53966183-114.3585025-30.53967594-114.3585055-30.53969035-114.3585084-30.53970548-114.3585075-30.53972176-114.3585092-30.53973927-114.3585098-30.53975816-114.3585114-30.53977746-114.35851-30.53979779-114.3585122-30.53981545-114.3585159-30.53983174-114.3585172-30.53984526-114.358513-30.53985872-114.3585092-30.53987189-114.3585027-30.53988631-114.3585023-30.53990214-114.3585062-30.53991719-114.3585137-30.53993272-114.3585186-30.539948-114.3585217-30.53996278-114.3585223-30.53997752-114.358526-30.53999293-114.358533-30.54000973-114.3585393-30.54002642-114.3585443-30.54004043-114.3585525-30.54005413-114.3585612-30.54006428-114.3585672-30.54007013-114.3585746-30.54007511-114.3585829-30.5400857-114.3585903-30.5401-114.3585954-30.54011478-114.3585977-30.54013168-114.3585991-30.54014865-114.3586028-30.5401659-114.3586073-30.54018308-114.3586118-30.54019977-114.3586151-30.54021333-114.3586213-30.54022513-114.3586298-30.54023678-114.358637-30.54024861-114.3586444-30.54026051-114.3586503-30.54027072-114.3586559-30.54027995-114.3586601-30.54029224-114.3586632-30.54030467-114.3586657-30.5403168-114.3586722-30.54032895-114.358679-30.54033933-114.3586886-30.54034856-114.3587-30.54035681-114.3587154-30.54036348-114.358733-30.54036962-114.358751-30.54037603-114.3587699-30.54038233-114.3587874-30.54038836-114.3588028-30.54039565-114.3588209-30.54040243-114.3588382-30.54040569-114.3588569-30.54040829-114.358876-30.54040957-114.3588941-30.54041307-114.3589139-30.54041888-114.3589317-30.54042628-114.3589482-30.54043185-114.3589661-30.539892-114.358477-30.540937-114.359138-30.54053301-114.3589331-30.54055473-114.3589416-30.5405699-114.3589518-30.54057564-114.358975-30.54057151-114.3590002-30.54056907-114.3590319-30.54056801-114.3590582-30.54056912-114.3590866-30.54057303-114.3590889-30.54057343-114.3590854-30.54057169-114.3590819-30.5405724-114.3590844-30.54058106-114.3590886-30.54058284-114.359095-30.54058775-114.3591046-30.54059042-114.3591131-30.54059454-114.3591278-30.54059549-114.3591408-30.54059719-114.3591514-30.54059807-114.3591652-30.54059753-114.3591776-30.54059347-114.3591904-30.54059044-114.3592027-30.54059008-114.3592148-30.54058855-114.3592273-30.54058564-114.359241-30.54058271-114.3592531-30.54058073-114.3592619-30.54057992-114.3592707-30.54058033-114.3592791-30.54058127-114.3592866-30.54058154-114.3592928-30.54058144-114.3592979-30.54057999-114.3593038-30.5405768-114.3593044-30.54057564-114.3593021-30.54057472-114.359302-30.54057456-114.3593034-30.54057457-114.3593057-30.5405744-114.3593082-30.54057719-114.3593084-30.540937-114.359138-30.54053211-114.3593354-30.54056622-114.3593416-30.54058638-114.3593375-30.54059603-114.3593457-30.54060404-114.3593514-30.54060841-114.3593576-30.54061338-114.3593645-30.5406206-114.3593715-30.54062745-114.3593786-30.54063551-114.3593852-30.54064441-114.3593908-30.54065383-114.3593961-30.54066369-114.3593999-30.54067263-114.3594031-30.54068229-114.3594062-30.54069274-114.3594103-30.54070205-114.3594157-30.540711-114.3594237-30.54071933-114.3594338-30.54072885-114.3594442-30.54073997-114.3594541-30.54075207-114.359463-30.54076335-114.3594676-30.54077663-114.3594731-30.54079023-114.3594781-30.54080501-114.3594832-30.54082165-114.3594887-30.54083916-114.3594933-30.54085663-114.359496-30.54087435-114.3594986-30.54089349-114.3595022-30.54091187-114.3595076-30.54092701-114.359514-30.54094264-114.3595182-30.54095981-114.3595206-30.54097597-114.3595201-30.5409911-114.359521-30.54100527-114.3595259-30.54102014-114.3595292-30.54103643-114.359528-30.54105325-114.3595208-30.54106817-114.35951-30.54108396-114.3595011-30.54109976-114.3594932-30.54111585-114.359487-30.54113183-114.3594817-30.54114594-114.3594768-30.54116019-114.3594705-30.54117318-114.3594639-30.54118525-114.3594558-30.54119736-114.3594502-30.5412083-114.3594449-30.54121833-114.3594468-30.54122885-114.3594466-30.54124155-114.3594425-30.54125277-114.359436-30.5412646-114.3594292-30.54127673-114.3594247-30.54129129-114.3594256-30.54130369-114.3594294-30.54131601-114.3594338-30.5413264-114.3594403-30.54133536-114.3594507-30.54134173-114.3594648-30.54134598-114.3594801-30.54135007-114.3594949-30.54135547-114.3595092-30.54136265-114.359523-30.54136941-114.3595343-30.54137735-114.359545-30.54138299-114.3595566-30.54138531-114.3595701-30.54138292-114.3595869-30.54138417-114.3596033-30.54138838-114.3596179-30.5413974-114.3596322-30.54140694-114.3596452-30.54141527-114.359658-30.54142162-114.3596724-30.54142649-114.3596881-30.5414302-114.3597043-30.54143019-114.3597204-30.54142692-114.3597371-30.54142105-114.3597536-30.54141304-114.359768-30.54140039-114.3597782-30.54138423-114.3597848-30.54137229-114.3597903-30.54136283-114.3597936-30.54135249-114.3597994-30.54134102-114.3598089-30.54132934-114.359818-30.54131456-114.3598308-30.54129924-114.3598426-30.54128307-114.3598556-30.54126889-114.3598648-30.5412552-114.3598744-30.54124195-114.3598808-30.54122879-114.3598869-30.54121502-114.3598957-30.54120352-114.3599063-30.54119429-114.3599166-30.54118461-114.3599256-30.54117206-114.3599317-30.54115652-114.3599346-30.54114035-114.3599351-30.54112246-114.3599348-30.54110517-114.3599352-30.54109052-114.3599318-30.54107645-114.3599239-30.54106413-114.3599168-30.54105118-114.3599131-30.54103847-114.3599141-30.54102645-114.3599152-30.54101479-114.3599169-30.54100219-114.3599213-30.54098939-114.3599257-30.54097734-114.3599297-30.54096602-114.35993-30.54095294-114.3599286-30.54093939-114.3599257-30.54092736-114.3599243-30.54091578-114.359925-30.54090321-114.3599283-30.54089102-114.3599345-30.5408798-114.3599412-30.54086966-114.3599485-30.54085826-114.3599543-30.54084759-114.3599572-30.54083625-114.35996-30.54082511-114.3599668-30.54081392-114.3599716-30.54080113-114.3599758-30.54078894-114.3599799-30.54077773-114.3599884-30.54076664-114.3599941-30.54075653-114.3600005-30.54074598-114.3600043-30.54073516-114.3600056-30.54072449-114.3600059-30.54071361-114.3600066-30.54070239-114.360009-30.54069292-114.3600131-30.54068276-114.3600175-30.54067243-114.3600214-30.54066188-114.360026-30.54065001-114.3600311-30.54063698-114.3600367-30.54062736-114.3600408-30.54061768-114.3600433-30.54060415-114.3600435-30.54059087-114.3600459-30.54057725-114.3600497-30.54056383-114.3600538-30.5405495-114.3600581-30.54053393-114.360062-30.54051871-114.3600667-30.54050424-114.3600728-30.54049028-114.3600799-30.54047666-114.3600879-30.54046448-114.3600973-30.54045183-114.3601083-30.54044408-114.3601171-30.54043572-114.3601261-30.5404337-114.3601444-30.54042496-114.3601641-30.5404112-114.3601766-30.54040636-114.3601822-30.54041144-114.3601899-30.5404132-114.3602046-30.54040597-114.3602194-30.54039332-114.3602323-30.54038186-114.3602458-30.54037215-114.3602596-30.54036503-114.3602738-30.54035962-114.3602869-30.54035097-114.3602973-30.54033996-114.3603052-30.54032743-114.3603114-30.54031594-114.3603192-30.54030456-114.3603274-30.54029592-114.3603381-30.54028797-114.3603498-30.54027914-114.3603634-30.5402652-114.3603753-30.54025242-114.3603878-30.54023872-114.3603834-30.54022814-114.360378-30.54021632-114.3603645-30.54020654-114.3603482-30.54019436-114.3603355-30.54018624-114.3603241-30.54017852-114.3603158-30.54016777-114.3603079-30.54015706-114.360298-30.54014779-114.360284-30.54013942-114.3602697-30.54012715-114.360255-30.54011059-114.3602392-30.54009737-114.3602261-30.54008306-114.3602139-30.5400663-114.3602017-30.54005259-114.36019-30.54004491-114.3601781-30.54004431-114.3601652-30.54004445-114.3601468-30.54004349-114.3601257-30.54004084-114.3601052-30.54003638-114.3600858-30.540032-114.3600681-30.54002907-114.3600489-30.54002558-114.3600311-30.54002555-114.360013-30.54002573-114.3599941-30.5400235-114.3599743-30.54002018-114.3599534-30.54001534-114.3599318-30.54000912-114.3599125-30.54000208-114.3598934-30.53999379-114.3598753-30.53998661-114.3598557-30.5399797-114.3598349-30.53997376-114.3598136-30.53996771-114.3597948-30.53996261-114.3597759-30.53995784-114.359759-30.53995044-114.3597432-30.53994552-114.359727-30.5399424-114.3597085-30.53993933-114.3596871-30.53993324-114.3596664-30.53992353-114.3596474-30.53991234-114.3596312-30.53990258-114.3596165-30.53989279-114.359602-30.53988307-114.3595871-30.53987302-114.3595727-30.53986322-114.3595601-30.53985315-114.3595498-30.53984718-114.3595431-30.53984362-114.3595319-30.5398409-114.3595208-30.53983518-114.3595082-30.53982466-114.359493-30.53981409-114.3594791-30.53980749-114.3594733-30.53980017-114.3594651-30.53979003-114.3594538-30.53978273-114.3594446-30.53977746-114.3594356-30.53977272-114.3594271-30.53976907-114.359418-30.53976013-114.359406-30.53974841-114.3593965-30.53973662-114.359388-30.53973027-114.3593751-30.53972767-114.3593622-30.53971903-114.3593524-30.53970862-114.3593443-30.53969925-114.3593346-30.53969018-114.3593253-30.5396812-114.3593165-30.53967226-114.359307-30.53966311-114.3592972-30.53965509-114.3592859-30.53964603-114.3592761-30.53963664-114.3592663-30.53962868-114.3592541-30.53962193-114.3592423-30.53961504-114.3592293-30.53960739-114.3592162-30.53959902-114.3592037-30.53958988-114.3591949-30.53958183-114.3591856-30.53957138-114.3591798-30.53956088-114.3591729-30.53954856-114.359169-30.53953624-114.3591646-30.53952114-114.35916-30.53950734-114.3591611-30.53949648-114.3591571-30.53948548-114.3591546-30.53947466-114.3591535-30.53946388-114.3591534');\n");
//            Insert insert = new InsertImpl(memConnect);
//            insert.insert(ins2);
//            insert.insert(ins1);
//        }catch(Exception e){
//            e.printStackTrace();
//        }

    }

    // 持久化轨迹
    public static void save(ArrayList<TrajectoryPoint> trajectory){
        if(trajectory == null || trajectory.size() == 0)
            return;
        int tID = getTrajectoryID();
        String uID = trajectory.get(0).userId;
        String tString = serialize(trajectory); // 序列化后的位置信息

        // 构造SQL语句
        String sql;
        if(tID % 2 == 1){
            sql = String.format("INSERT INTO mobile_phone_traj VALUES (%d,'%s','%s');", tID, uID, tString);
        }else{
            sql = String.format("INSERT INTO watch_traj VALUES (%d,'%s','%s');", tID, uID, tString);
        }

        // 执行SQl语句
        try {
            Insert insert = new InsertImpl(memConnect);
            Statement parse = CCJSqlParserUtil.parse(sql);
            insert.insert(parse);
        }catch (Throwable e){
            e.printStackTrace();
        }

    }

    // 读取历史轨迹数据，每条轨迹是若干个轨迹点组成的ArrayList
    public static ArrayList<ArrayList<TrajectoryPoint>> load(boolean mobile, boolean watch, boolean join){
        ArrayList<ArrayList<TrajectoryPoint>> ret = new ArrayList<>();
        String sql1 = "SELECT * FROM mobile_phone_traj;";
        String sql2 = "SELECT * FROM watch_traj;";
        String sql3 = "SELECT * FROM join_traj";
        try{
            Select select = new SelectImpl(memConnect);
            if (mobile) {
                Statement parse1 = CCJSqlParserUtil.parse(sql1);
                SelectResult result1 = select.select(parse1);
                for(Tuple t: result1.getTpl().tuplelist){
                    ret.add(deserialize((String) t.tuple[2]));
                }
            }
            if (watch) {
                Statement parse2 = CCJSqlParserUtil.parse(sql2);
                SelectResult result2 = select.select(parse2);
                for(Tuple t: result2.getTpl().tuplelist){
                    ret.add(deserialize((String) t.tuple[2]));
                }
            }
            if (join) {
                Statement parse3 = CCJSqlParserUtil.parse(sql3);
                SelectResult result3 = select.select(parse3);
                for(Tuple t: result3.getTpl().tuplelist){
                    ret.add(deserialize((String) t.tuple[2]));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }


    // 获取TrajectoryID
    public static int getTrajectoryID(){
        int tid = -1;
        try{
            File f = new File("/data/data/drz.tmdb/tid");
            // 首次创建则tid为1
            if(!f.exists()){
                f.createNewFile();
                RandomAccessFile raf = new RandomAccessFile(f, "rw");
                raf.writeInt(1);
                tid = 1;
            }
            // 每次tid递增1
            else{
                RandomAccessFile raf = new RandomAccessFile(f, "rw");
                tid = raf.readInt() + 1;
                raf.seek(0);
                raf.writeInt(tid);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return tid;
    }


    // 将轨迹序列化
    public static String serialize(ArrayList<TrajectoryPoint> trajectory){
        if(trajectory == null || trajectory.size() == 0)
            return "";
        StringBuilder ret = new StringBuilder();
        for(TrajectoryPoint point : trajectory){
            ret.append(point.longitude);
            ret.append("-");
            ret.append(point.latitude);
            ret.append("-");
        }
        return ret.toString().substring(0, ret.length()-1);
//        if(trajectory == null || trajectory.size() == 0)
//            return "";
//        StringBuilder ret = new StringBuilder();
//        for(TrajectoryPoint point : trajectory){
//            ret.append(new String(double2Bytes(point.longitude)));
//            ret.append(new String(double2Bytes(point.latitude)));
//        }
//        return ret.toString();
    }

    // 将String反序列化成轨迹
    public static ArrayList<TrajectoryPoint> deserialize(String str){
        ArrayList<TrajectoryPoint> ret = new ArrayList<>();
        String[] info = str.replace("'", "").split("-");
        int pointCount = info.length / 2;
        for(int i=0; i<pointCount; i++){
            ret.add(new TrajectoryPoint(Double.parseDouble(info[i]), Double.parseDouble(info[i+1])));
        }
        return ret;
//        ArrayList<TrajectoryPoint> ret = new ArrayList<>();
//        int pointCount = str.length() / 16;
//        for(int i=0; i<pointCount; i++){
//            byte[] lo = str.substring(8 * i, 8 * i + 8).getBytes();
//            byte[] la = str.substring(8 * i + 8, 8 * i + 16).getBytes();
//            ret.add(new TrajectoryPoint(bytes2Double(lo), bytes2Double(la)));
//        }
//        return ret;
    }

    private static byte[] double2Bytes(double d) {
        byte[] data = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try{
            dos.writeDouble(d);
            dos.flush();
            data = bos.toByteArray();
            dos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return data;
    }

    public static double bytes2Double(byte[] arr){
        double num = 0;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(arr));
        try{
            num = dis.readDouble();
            dis.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return num;
    }

}
