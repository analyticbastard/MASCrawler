using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;

using ConsoleApplication1.ServiceReference1;
using System.Data.SqlServerCe;


namespace MASCrawler
{
    class Program
    {
        static long downloadDomains(SqlCeConnection con, APIServiceClient client,
            long maxauthdomid)
        {
            if (maxauthdomid == -1)
            {
                Console.WriteLine("Donwloading domains...");
                List<Domain> domains = MAS.masGetDomains(client);

                DB.dbInsertDomains(con, domains);

                Console.WriteLine("Donwloading subdomains...");
                foreach (var d in domains)
                {
                    List<Domain> subdomains = MAS.masGetSubDomains(client, d);
                    DB.dbInsertSubDomains(con, d, subdomains);
                }

                maxauthdomid = 2;
            }

            return maxauthdomid;
        }

        static void downloadAuthors(SqlCeConnection con, APIServiceClient client,
            long maxauthdomid)
        {
            for (uint id = (uint)maxauthdomid; id < 25; id++)
            {
                Domain d = new Domain();
                d.DomainID = id;

                List<Domain> subdomains = DB.dbGetSubDomains(con, d);

                foreach (var s in subdomains)
                {
                    List<Author> authors = new List<Author>();

                    Console.WriteLine("Downloading authors for subdomain " +
                        id + "-" + s.SubDomainID + s.Name);
                    /* !!! */
                    //if (s.SubDomainID < 11)
                    //    continue;

                    uint i = 0;
                    do
                    {
                        authors.Clear();
                        MAS.masGetAuthors(client, id, s.SubDomainID, i, authors);
                        DB.dbInsertAuthors(con, s, authors);
                        i++;
                    } while (authors.Count > 0);
                }
            }

            GC.Collect();
        }

        static void downloadOrganizations(SqlCeConnection con, APIServiceClient client)
        {
        }

        static void downloadPapers(SqlCeConnection con, APIServiceClient client, Domain slast)
        {
            List<Domain> domains = DB.dbGetDomains(con);

            if (domains[0].DomainID == 1)
                domains.Remove(domains[0]);  // Remove multidisciplinary

            bool loop = false;

            foreach (var d in domains)
            {
                List<Domain> subdomains = DB.dbGetSubDomains(con, d);

                foreach (var s in subdomains)
                {
                    if (slast.DomainID == 0 && slast.SubDomainID == 0)
                        loop = true;
                    else if (s.DomainID == slast.DomainID && s.SubDomainID == slast.SubDomainID)
                        loop = true;

                    Console.WriteLine("Downloading publications in " + 
                        s.DomainID + "-" + s.SubDomainID);

                    if (!loop)
                        continue;
                    
                    List<Author> authors = DB.dbGetAuthors(con, s);
                    List<Publication> publications = null;
                    foreach (var a in authors)
                    {
                        List<Domain> author_domains = DB.dbGetAuthorDomains(con, a);

                        Domain ad1 = null;

                        foreach (Domain ad in author_domains)
                        {
                            ad1 = ad;

                            if (ad.DomainID < d.DomainID)
                                break;
                            if (ad.DomainID == d.DomainID && ad.SubDomainID < d.SubDomainID)
                                break;
                        }

                        if (ad1.DomainID < d.DomainID)
                            continue;
                        if (ad1.DomainID == d.DomainID && ad1.SubDomainID < d.SubDomainID)
                            continue;

                        publications = MAS.masGetPublications(client, s.DomainID,
                            s.SubDomainID, a.ID);
                        
                        List<Publication> inserted = new List<Publication>();

                        DB.dbInsertPublications(con, s, a, publications, inserted);
                        DB.dbInsertCoauthors(con, a, publications);
                        try { CSV.csvInsert(inserted); }
                        catch (Exception e) { }
                    }
                }
            }

            GC.Collect();
        }

        static void downloadPapers(SqlCeConnection con, APIServiceClient client)
        {
            List<Domain> domains = DB.dbGetDomains(con);

            if (domains[0].DomainID == 1)
                domains.Remove(domains[0]);  // Remove multidisciplinary

            Domain slast = DB.dbGetLastPublication(con);

            downloadPapers(con, client, slast);
        }

        static void downloadCitations(SqlCeConnection con, APIServiceClient client)
        {
            /*authorcitations = MAS.masGetAuthorCitations(client, s.DomainID,
            s.SubDomainID, a.ID);*/
            List<Domain> domains = DB.dbGetDomains(con);
            foreach (var d in domains) {
                List<Domain> subdomains = DB.dbGetSubDomains(con, d);
                foreach (var s in subdomains)
                {
                    List<Author> authors = DB.dbGetAuthors(con, s);
                    foreach (var a in authors)
                    {
                        List<Publication> publications = DB.dbGetPublications(con, a);
                        foreach (var p in publications)
                        {
                            List<Publication> citing = MAS.masGetPublicationCitations(client, p.ID);
                            DB.dbInsertCitations(con, p, citing);
                        }
                    }
                }
            }
        }


        static void Main(string[] args)
        {
            SqlCeConnection con = null;

            try
            {
                APIServiceClient client = new APIServiceClient();

                con =
                    DB.dbOpen(Properties.Settings.Default.datosConnectionString);

                if (Properties.Settings.Default.state < 5)
                {
                    DB.dbAdjustAllTables(con);

                    long maxauthdomid = DB.dbGetLastAuthor(con);

                    Console.WriteLine("Database and MAS opened");

                    maxauthdomid = downloadDomains(con, client, maxauthdomid);

                    downloadAuthors(con, client, maxauthdomid);

                    Properties.Settings.Default.state = 5;
                }
                
                if (Properties.Settings.Default.state < 10)
                    downloadPapers(con, client);

                downloadCitations(con, client);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        
            DB.dbClose(con);
        }
    }
}


