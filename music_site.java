import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Assignment {
	public void chart(Connection conn) throws ClassNotFoundException, SQLException{
		try {
			Scanner sc = new Scanner(System.in);
			List<String> DATE = new ArrayList<>();
			Statement stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery("select distinct DATE(Datetime) DAY from Listen_log");
			System.out.println("차트 기록 날짜 목록");
			while(rset.next()) {
				System.out.println(rset.getString("DAY"));
				DATE.add(rset.getString("DAY"));
			}
			String date="";
			System.out.println("차트 기록을 보고싶은 날짜를 정확히 입력하세요(예:2021-10-30)");
			date=sc.nextLine();
			Boolean IsContains = DATE.contains(date);
			if(!IsContains) {
				while(!IsContains) {
					System.out.println("다시 입력하세요");
					date=sc.nextLine();
					IsContains = DATE.contains(date);
					if (IsContains) {
						break;
					}
				}
				
			}
			
			String sql="select Music.Music_ID from Listen_log,Music where Listen_log.Music_ID=Music.Music_ID and DATE(Listen_log.Datetime)=? group by Listen_log.Music_ID order by count(*) desc limit 100;";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, date);
			rset = pstmt.executeQuery();
			
			int i = 1;
			while(rset.next()) {
				System.out.print(i+"위 "+"ID:"+rset.getString("Music_ID")+" ");
				Statement stmtSub = conn.createStatement();
				String subSql="select Music.title from Music natural join Sing where Sing.Music_ID=?;";
				PreparedStatement pstmtSub=conn.prepareStatement(subSql);
				pstmtSub.setString(1, rset.getString("Music_ID"));
				ResultSet rsetSub = pstmtSub.executeQuery();
				if(rsetSub.next()) {
					System.out.print("Title:"+rsetSub.getString("Title")+" ");
				}
				System.out.print("Artist:");
				subSql="select Artist.name from Artist natural join Sing where Sing.Music_ID=?;";
				pstmtSub=conn.prepareStatement(subSql);
				pstmtSub.setString(1, rset.getString("Music_ID"));
				rsetSub = pstmtSub.executeQuery();
				while(rsetSub.next()){
					System.out.print(rsetSub.getString("name"));
					if (!rsetSub.isLast()) {
						System.out.print(", ");
					}
				}
				stmtSub.close();
				rsetSub.close();
				System.out.println("");
				i++;
			}
			
			stmt.close();
		}catch(SQLException sqle) {
			System.out.println("SQLException :"+sqle);
		}
	}
	
	public void recommend(Connection conn) throws ClassNotFoundException, SQLException{
		try {
			Statement stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery("select music.Music_ID, music.title from music");
			List<String> ID = new ArrayList<>();
			while(rset.next()) {
				System.out.print("ID:"+rset.getString("Music_ID")+" ");
				System.out.print("Title:"+rset.getString("Title")+" ");
				System.out.print("Artist:");
				Statement stmtSub = conn.createStatement();
				String subSql="select Artist.name from Artist natural join Sing where Sing.Music_ID=?;";
				PreparedStatement pstmtSub=conn.prepareStatement(subSql);
				pstmtSub.setString(1, rset.getString("Music_ID"));
				ResultSet rsetSub = pstmtSub.executeQuery();
				while(rsetSub.next()){
					System.out.print(rsetSub.getString("name"));
					if (!rsetSub.isLast()) {
						System.out.print(", ");
					}
				}
				stmtSub.close();
				rsetSub.close();
				System.out.println("");
				ID.add(rset.getString("Music_ID"));
			}
			
			Scanner sc = new Scanner(System.in);
			String MusicTitle = " ";
			String MusicID = " ";
			System.out.println("원하는 곡의 ID를 정확히 입력해주세요");
			MusicID = sc.nextLine();
			Boolean IsContains = ID.contains(MusicID);
			if(!IsContains) {
				while(!IsContains) {
					System.out.println("다시 입력하세요");
					MusicID=sc.next();
					IsContains = ID.contains(MusicID);
					if (IsContains) {
						break;
					}
				}
				
			}
			String sql="select title from music where Music_ID=?;";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, MusicID);
			rset = pstmt.executeQuery();
			if (rset.next()){
				MusicTitle=rset.getString("title");
			}
			
			sql="select distinct A.Music_ID from music A, artist B, sing D, compose E, lyric F, arrange G\r\n"
					+ "where A.Music_ID=D.Music_ID \r\n"
					+ "and A.Music_ID=E.Music_ID\r\n"
					+ "and A.Music_ID=F.Music_ID\r\n"
					+ "and A.Music_ID=G.Music_ID\r\n"
					+ "and D.Artist_ID=B.Artist_ID\r\n"
					+ "and (D.Artist_ID in (select Artist_ID from music, sing where music.Music_ID = sing.Music_ID and music.Music_ID=?)\r\n"
					+ "or E.Artist_ID in (select Artist_ID from music, compose where music.Music_ID = compose.Music_ID and music.Music_ID=?)\r\n"
					+ "or F.Artist_ID in (select Artist_ID from music, lyric where music.Music_ID = lyric.Music_ID and music.Music_ID=?)\r\n"
					+ "or G.Artist_ID in (select Artist_ID from music, arrange where music.Music_ID = arrange.Music_ID and music.Music_ID=?)\r\n"
					+ "or D.Artist_ID in (select Artist_ID from music, sing where music.Music_ID = sing.Music_ID and music.Music_ID=?)\r\n"
					+ "or A.Genre in (select Genre from music where Music_ID=?))\r\n"
					+ "and A.Music_ID !=?\r\n"
					+ "limit 10;";
			pstmt=conn.prepareStatement(sql);
			pstmt.setString(1, MusicID);
			pstmt.setString(2, MusicID);
			pstmt.setString(3, MusicID);
			pstmt.setString(4, MusicID);
			pstmt.setString(5, MusicID);
			pstmt.setString(6, MusicID);
			pstmt.setString(7, MusicID);
			rset = pstmt.executeQuery();
			
			System.out.println(MusicTitle+"의 유사곡 추천");
			while(rset.next()) {
				System.out.print("ID:"+rset.getString("Music_ID")+" ");
				Statement stmtSub = conn.createStatement();
				String subSql="select Music.title from Music natural join Sing where Sing.Music_ID=?;";
				PreparedStatement pstmtSub=conn.prepareStatement(subSql);
				pstmtSub.setString(1, rset.getString("Music_ID"));
				ResultSet rsetSub = pstmtSub.executeQuery();
				if(rsetSub.next()) {
					System.out.print("Title:"+rsetSub.getString("Title")+" ");
				}
				System.out.print("Artist:");
				subSql="select Artist.name from Artist natural join Sing where Sing.Music_ID=?;";
				pstmtSub=conn.prepareStatement(subSql);
				pstmtSub.setString(1, rset.getString("Music_ID"));
				rsetSub = pstmtSub.executeQuery();
				while(rsetSub.next()){
					System.out.print(rsetSub.getString("name"));
					if (!rsetSub.isLast()) {
						System.out.print(", ");
					}
				}
				stmtSub.close();
				rsetSub.close();
				System.out.println("");
			}
			
			stmt.close();
		} catch(SQLException sqle) {
			System.out.println("SQLException :"+sqle);
		}
		
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		try {
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/assignment?useUnicode=true&useJDBCCompliantTimezoneShift=true&"+
		"useLagacyDtetimeCode=false&serverTimezone=UTC","root","1234");
			
			Scanner sc = new Scanner(System.in);
			Assignment assignment = new Assignment();
			int func=0;
			
			while(func!=3){
				System.out.println("기능을 선택하세요");
				System.out.println("1.차트 기록");
				System.out.println("2.유사곡 추천");
				System.out.println("3.종료");
				System.out.println(" ");
				func = sc.nextInt();
				
				if (func==1) {
					assignment.chart(conn);
					System.out.println("");
				}
				else if (func==2) {
					assignment.recommend(conn);
					System.out.println("");
				}
				else if (func==3) {
					break;
				}
			}
			conn.close();
					
		}
		catch(SQLException sqle) {
			System.out.println("SQLException :"+sqle);
		}
	}
}
