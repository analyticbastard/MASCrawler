using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Data;

using ConsoleApplication1.ServiceReference1;

namespace MASCrawler
{
    class DB
    {
        public static SqlCeConnection dbOpen(String conString)
        {
            SqlCeConnection con = null;
            try
            {
                con = new SqlCeConnection(conString);
                con.Open();
            }
            catch (SqlCeException e)
            {

            }
            return con;
        }

        public static void dbClose(SqlCeConnection con)
        {
            if (con != null)
                con.Close();
        }

        public static long dbGetLastAuthor(SqlCeConnection con)
        {
            String sql = "SELECT * FROM SubDomain_Authors";

            try
            {
                SqlCeCommand cmd = new SqlCeCommand(sql, con);

                SqlCeResultSet rs = cmd.ExecuteResultSet(ResultSetOptions.Scrollable);
                if (rs.HasRows)
                {
                    int ordID = rs.GetOrdinal("DomainID");
                    rs.ReadLast();
                    return rs.GetInt64(ordID);
                }
            }
            catch (Exception e)
            {

            }

            return -1;
        }

        public static List<Domain> dbGetDomains(SqlCeConnection con)
        {
            String sql = "SELECT * FROM Domain";

            List<Domain> domains = new List<Domain>();
            try
            {
                SqlCeCommand cmd = new SqlCeCommand(sql, con);

                SqlCeResultSet rs = cmd.ExecuteResultSet(ResultSetOptions.Scrollable);
                if (rs.HasRows)
                {
                    int ordID = rs.GetOrdinal("ID");
                    int ordTitle = rs.GetOrdinal("Title");
                    rs.ReadFirst();
                    Domain d = null;
                    do
                    {
                        d = new Domain();
                        d.DomainID = (uint)rs.GetInt64(ordID);
                        d.Name = rs.GetString(ordTitle);
                        domains.Add(d);
                    } while (rs.Read());
                }
            }
            catch (Exception e)
            {
            }

            return domains;
        }

        public static Domain dbGetLastPublication(SqlCeConnection con)
        {
            String sql = "SELECT * FROM SubDomain_Publication";

            Domain d = new Domain();

            try
            {
                SqlCeCommand cmd = new SqlCeCommand(sql, con);

                SqlCeResultSet rs = cmd.ExecuteResultSet(ResultSetOptions.Scrollable);
                if (rs.HasRows)
                {
                    int ordID = rs.GetOrdinal("DomainID");
                    int ordSubID = rs.GetOrdinal("SubDomainID");
                    //int ordPubID = rs.GetOrdinal("PublicationID");

                    rs.ReadLast();

                    d.DomainID = (uint)rs.GetInt64(ordID);
                    d.SubDomainID = (uint)rs.GetInt64(ordSubID);
                }
            }
            catch (Exception e)
            {
            }

            return d;
        }

        public static List<Domain> dbGetSubDomains(SqlCeConnection con, Domain domain)
        {
            String sql = "SELECT * FROM SubDomain WHERE ID=" + domain.DomainID;

            List<Domain> subdomains = new List<Domain>();
            try
            {
                SqlCeCommand cmd = new SqlCeCommand(sql, con);

                SqlCeResultSet rs = cmd.ExecuteResultSet(ResultSetOptions.Scrollable);
                if (rs.HasRows)
                {
                    int ordID = rs.GetOrdinal("ID");
                    int ordSubID = rs.GetOrdinal("SubID");
                    int ordTitle = rs.GetOrdinal("Title");
                    rs.ReadFirst();
                    Domain d = null;
                    do
                    {
                        d = new Domain();
                        d.DomainID = (uint)rs.GetInt64(ordID);
                        d.SubDomainID = (uint)rs.GetInt64(ordSubID);
                        d.Name = rs.GetString(ordTitle);
                        subdomains.Add(d);
                    } while (rs.Read());
                }
            }
            catch (Exception e)
            {

            }

            return subdomains;
        }

        public static List<Author> dbGetAuthors(SqlCeConnection con, Domain subdomain)
        {
            /*String sql = "SELECT * FROM Authors WHERE (ID IN " + 
                "(SELECT AuthorID FROM SubDomain_Authors WHERE (DomainID="  + 
                subdomain.DomainID + ") AND (SubDomainID=" + subdomain.SubDomainID + ")))";*/
            String sql = "SELECT Authors.* FROM Authors INNER JOIN " +
                    "(SELECT AuthorID FROM SubDomain_Authors " +
                    "WHERE (DomainID = " + subdomain.DomainID + 
                    ") AND (SubDomainID = " + subdomain.SubDomainID + 
                    ")) AS t ON Authors.ID = t.AuthorID";

            List<Author> authors = new List<Author>();
            try
            {
                SqlCeCommand cmd = new SqlCeCommand(sql, con);

                SqlCeResultSet rs = cmd.ExecuteResultSet(ResultSetOptions.Scrollable);
                if (rs.HasRows)
                {
                    int ordID = rs.GetOrdinal("ID");
                    int ordFName = rs.GetOrdinal("First Name");
                    int ordMName = rs.GetOrdinal("Middle Name");
                    int ordLName = rs.GetOrdinal("Last Name");
                    int ordGIndex = rs.GetOrdinal("GIndex");
                    int ordHIndex = rs.GetOrdinal("HIndex");
                    rs.ReadFirst();
                    Author a = null;
                    do
                    {
                        a = new Author();
                        a.ID = (uint)rs.GetInt64(ordID);
                        a.FirstName = rs.GetString(ordFName);
                        a.MiddleName = rs.GetString(ordMName);
                        a.LastName = rs.GetString(ordLName);
                        a.GIndex = (uint) rs.GetInt16(ordGIndex);
                        a.HIndex = (uint)rs.GetInt16(ordHIndex);
                        authors.Add(a);
                    } while (rs.Read());
                }
            }
            catch (Exception e)
            {

            }

            return authors;
        }

        public static List<Publication> dbGetPublications(SqlCeConnection con, Author author)
        {
            /*String sql = "SELECT * FROM Authors WHERE (ID IN " + 
                "(SELECT AuthorID FROM SubDomain_Authors WHERE (DomainID="  + 
                subdomain.DomainID + ") AND (SubDomainID=" + subdomain.SubDomainID + ")))";*/
            String sql = "SELECT ID FROM Publication INNER JOIN " +
                    "(SELECT PublicationID FROM Authors_Publication " +
                    "WHERE (AuthorID = " + author.ID + ")) AS t ON Publication.ID = t.PublicationID";

            List<Publication> publications = new List<Publication>();
            try
            {
                SqlCeCommand cmd = new SqlCeCommand(sql, con);

                SqlCeResultSet rs = cmd.ExecuteResultSet(ResultSetOptions.Scrollable);
                if (rs.HasRows)
                {
                    int ordID = rs.GetOrdinal("ID");
                    rs.ReadFirst();
                    Publication p = null;
                    do
                    {
                        p = new Publication();
                        p.ID = (uint)rs.GetInt64(ordID);
                        publications.Add(p);
                    } while (rs.Read());
                }
            }
            catch (Exception e)
            {
            }

            return publications;
        }

        public static List<Domain> dbGetAuthorDomains(SqlCeConnection con, Author author)
        {
            String sql = "SELECT DomainID, SubDomainID FROM Authors_Publication INNER JOIN " +
                   "(SELECT AuthorID FROM SubDomain_Authors " +
                   "WHERE (AuthorID = " + author.ID + ")) AS t ON " +
                   "Authors_Publication.ID = t.AuthorID";

            List<Domain> domains = new List<Domain>();
            try
            {
                SqlCeCommand cmd = new SqlCeCommand(sql, con);

                SqlCeResultSet rs = cmd.ExecuteResultSet(ResultSetOptions.Scrollable);
                if (rs.HasRows)
                {
                    int ordDomID = rs.GetOrdinal("DomainID");
                    int ordSubID = rs.GetOrdinal("SubDomainID");
                    rs.ReadFirst();
                    Domain d = null;
                    do
                    {
                        d = new Domain();
                        d.DomainID = (uint)rs.GetInt64(ordDomID);
                        d.SubDomainID = (uint)rs.GetInt64(ordSubID);
                        domains.Add(d);
                    } while (rs.Read());
                }
            }
            catch (Exception e) { }

            return domains;
        }


        public static void dbInsertDomains(SqlCeConnection con, List<Domain> domains)
        {
            foreach (var d in domains)
            {
                using (SqlCeCommand com = new SqlCeCommand("INSERT INTO Domain VALUES(@ID,@Title)", con))
                {
                    try
                    {
                        com.Parameters.AddWithValue("@ID", d.DomainID);
                        com.Parameters.AddWithValue("@Title", d.Name);
                        com.ExecuteNonQuery();
                    }
                    catch (SqlCeException e)
                    {

                    }
                }
            }
        }

        public static void dbInsertSubDomains(SqlCeConnection con, Domain domain, List<Domain> subdomains)
        {
            foreach (var s in subdomains)
            {
                using (SqlCeCommand com = new SqlCeCommand("INSERT INTO SubDomain VALUES(@ID,@SubID,@Title)", con))
                {
                    try
                    {
                        com.Parameters.AddWithValue("@ID", s.DomainID);
                        com.Parameters.AddWithValue("@SubID", s.SubDomainID);
                        com.Parameters.AddWithValue("@Title", s.Name);
                        com.ExecuteNonQuery();
                    }
                    catch (SqlCeException e)
                    {
                    }
                }
            }
        }

        public static void dbInsertAuthors(SqlCeConnection con, Domain subdomain, List<Author> authors)
        {
            foreach (var a in authors)
            {
                try
                {
                    using (SqlCeCommand com = new SqlCeCommand(
                        "INSERT INTO Authors VALUES(@ID,@FirstName,@MiddleName,@LastName,@HIndex,@GIndex)",
                        con))
                    {
                        com.Parameters.AddWithValue("@ID", a.ID);
                        com.Parameters.AddWithValue("@FirstName",
                            a.FirstName.Substring(0, Math.Min(a.FirstName.Length, 10)));
                        com.Parameters.AddWithValue("@MiddleName",
                            a.MiddleName.Substring(0, Math.Min(a.MiddleName.Length, 10)));
                        com.Parameters.AddWithValue("@LastName",
                            a.LastName.Substring(0, Math.Min(a.LastName.Length, 10)));
                        com.Parameters.AddWithValue("@HIndex", a.HIndex);
                        com.Parameters.AddWithValue("@GIndex", a.GIndex);
                        com.ExecuteNonQuery();
                    }
                }
                catch (SqlCeException e)
                {

                }
                try
                {
                    using (SqlCeCommand com = new SqlCeCommand(
                        "INSERT INTO SubDomain_Authors VALUES(@DomainID,@SubDomainID,@AuthorID)",
                        con))
                    {
                        com.Parameters.AddWithValue("@DomainID", subdomain.DomainID);
                        com.Parameters.AddWithValue("@SubDomainID", subdomain.SubDomainID);
                        com.Parameters.AddWithValue("@AuthorID", a.ID);
                        com.ExecuteNonQuery();
                    }
                }
                catch (SqlCeException e)
                {
                }
            }
        }

        public static void dbInsertPublications(SqlCeConnection con, 
            Domain subdomain, Author author, List<Publication> publications,
            List<Publication> inserted)
        {
            foreach (var p in publications)
            {
                try
                {
                    using (SqlCeCommand com = new SqlCeCommand(
                        "INSERT INTO Publication VALUES(@ID)", con))
                    {
                        com.Parameters.AddWithValue("@ID", p.ID);
                        com.ExecuteNonQuery();
                        inserted.Add(p);
                    }
                }
                catch (Exception e)
                {
                }
                try
                {
                    using (SqlCeCommand com = new SqlCeCommand(
                        "INSERT INTO Authors_Publication VALUES(@AuthorID,@PublicationID)", con))
                    {
                        com.Parameters.AddWithValue("@AuthorID", author.ID);
                        com.Parameters.AddWithValue("@PublicationID", p.ID);
                        com.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                }
                try
                {
                    using (SqlCeCommand com = new SqlCeCommand(
                        "INSERT INTO SubDomain_Publication VALUES(@DomainID,@SubDomainID,@PublicationID)", con))
                    {
                        com.Parameters.AddWithValue("@DomainID", subdomain.DomainID);
                        com.Parameters.AddWithValue("@SubDomainID", subdomain.SubDomainID);
                        com.Parameters.AddWithValue("@PublicationID", p.ID);
                        com.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                }
            }
        }

        public static void dbInsertCitations(SqlCeConnection con,
            Publication publication, List<Publication> publications)
        {
            foreach (var p in publications)
            {
                try
                {
                    using (SqlCeCommand com = new SqlCeCommand(
                        "INSERT INTO Publication_Publication VALUES(@ID1,@ID2)", con))
                    {
                        com.Parameters.AddWithValue("@ID1", publication.ID);
                        com.Parameters.AddWithValue("@ID2", p.ID);
                        com.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                }
            }
        }

        public static void dbInsertCoauthors(SqlCeConnection con,
            Author author, List<Publication> publications)
        {
            foreach (var p in publications)
            {
                //string sql = "INSERT INTO Authors_Authors VALUES(@ID1,@ID2)";
                string sql = "INSERT INTO Authors_Authors VALUES(" + author.ID + ",";
                string sqlcom = null;

                foreach (var a in p.Author)
                {
                    if (a.ID == author.ID)
                        continue;

                    try
                    {
                        sqlcom = sql + a.ID + ")";
                        using (SqlCeCommand com = new SqlCeCommand(sqlcom, con))
                        {
                            //com.Parameters.AddWithValue("@ID1", author.ID);
                            //com.Parameters.AddWithValue("@ID2", a.ID);
                            com.ExecuteNonQuery();
                        }
                    }
                    catch (Exception e)
                    {
                    }
                }
            }
        }
        
        

        public static void dbAdjustSubDomain(SqlCeConnection con)
        {
            try
            {
                using (SqlCeCommand com = new SqlCeCommand("ALTER TABLE SubDomain ADD PRIMARY KEY (ID, SubID)", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
            try
            {
                using (SqlCeCommand com = new SqlCeCommand("ALTER TABLE SubDomain ADD FOREIGN KEY (ID) REFERENCES Domain(ID)", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
        }

        public static void dbAdjustSubDomain_Authors(SqlCeConnection con)
        {
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE SubDomain_Authors ADD PRIMARY KEY (DomainID, SubDomainID, AuthorID)", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {

            }
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE SubDomain_Authors ADD FOREIGN KEY (DomainID,SubDomainID) REFERENCES SubDomain(ID,SubID) ON DELETE CASCADE;", con))
                {
                    com.ExecuteNonQuery();
                }
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE SubDomain_Authors ADD FOREIGN KEY (AuthorID) REFERENCES Authors(ID) ON DELETE CASCADE;", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {

            }
        }


        public static void dbAdjustAuthors_Authors(SqlCeConnection con)
        {
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Authors_Authors ADD PRIMARY KEY (ID1,ID2)", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Authors_Authors ADD FOREIGN KEY (ID1) REFERENCES Authors(ID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Authors_Authors ADD FOREIGN KEY (ID2) REFERENCES Authors(ID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
        }

        public static void dbAdjustAuthors_Publication(SqlCeConnection con)
        {
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Authors_Publication ADD PRIMARY KEY (AuthorID, PublicationID)", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (SqlCeException e)
            {
            }
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Authors_Publication ADD FOREIGN KEY (AuthorID) REFERENCES Authors(ID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Authors_Publication ADD FOREIGN KEY (PublicationID) REFERENCES Publication(ID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
        }

        public static void dbAdjustSubDomain_Publication(SqlCeConnection con)
        {
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE SubDomain_Publication ADD PRIMARY KEY (DomainID, SubDomainID, PublicationID)", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE SubDomain_Publication ADD FOREIGN KEY (DomainID,SubDomainID) REFERENCES SubDomain(ID,SubID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE SubDomain_Publication ADD FOREIGN KEY (PublicationID) REFERENCES Publication(ID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
        }

        public static void dbAdjustPublication_Publication(SqlCeConnection con)
        {
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Publication_Publication ADD PRIMARY KEY (ID1, ID2)", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
            try
            {
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Publication_Publication ADD FOREIGN KEY (ID1) REFERENCES Publication(ID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
                using (SqlCeCommand com = new SqlCeCommand(
                    "ALTER TABLE Publication_Publication ADD FOREIGN KEY (ID2) REFERENCES Publication(ID) ON DELETE CASCADE", con))
                {
                    com.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
            }
        }

        public static void dbAdjustAllTables(SqlCeConnection con)
        {
            DB.dbAdjustPublication_Publication(con);
            DB.dbAdjustAuthors_Publication(con);
            DB.dbAdjustAuthors_Authors(con);
            DB.dbAdjustSubDomain(con);
            DB.dbAdjustSubDomain_Authors(con);
            DB.dbAdjustSubDomain_Publication(con);
        }


    }
}
