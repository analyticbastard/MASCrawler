using System;
using System.Collections.Generic;
using System.Collections;

using ConsoleApplication1.ServiceReference1;


namespace MASCrawler
{
    class MAS
    {
        static String AppID = "9a03ffda-79b9-43e7-ac66-52feaa9a2041";

        const uint MAS_MINHINDEX = 5;
        const uint MAS_MINGINDEX = 7;
        const int MAS_DELAYMS = 333;

        public static List<Domain> masGetDomains(APIServiceClient client)
        {
            List<Domain> domains = new List<Domain>();

            try
            {
                Request request = new Request();
                request.AppID = AppID;
                request.ResultObjects = ObjectType.Domain;
                Boolean inloop = true;
                uint i = 1;
                while (inloop)
                {
                    request.DomainID = i;
                    request.SubDomainID = 0;
                    Response response = client.Search(request);
                    if (response.Domain == null)
                    {
                        return domains;
                    }
                    if (response.Domain.Result == null)
                    {
                        inloop = false;
                    }
                    foreach (var d in response.Domain.Result)
                    {
                        if (d.Name != null)
                        {
                            if (!d.Name.Equals(""))
                                domains.Add(d);
                        }
                    }
                    i++;
                    if (i > 25)
                        inloop = false;
                }
            }
            catch (Exception e)
            {

            }

            return domains;
        }

        public static List<Domain> masGetSubDomains(APIServiceClient client, Domain domain)
        {
            List<Domain> subdomains = new List<Domain>();

            try
            {
                Request request = new Request();
                request.AppID = AppID;
                request.ResultObjects = ObjectType.Domain;
                Console.WriteLine(domain.Name);
                Boolean inloop = true;
                uint i = 1;
                while (inloop)
                {
                    request.DomainID = domain.DomainID;
                    request.SubDomainID = i;
                    Response response = client.Search(request);
                    if (response.Domain == null)
                    {
                        inloop = false;
                        return subdomains;
                    }
                    if (response.Domain.Result == null)
                    {
                        inloop = false;
                    }
                    foreach (var d in response.Domain.Result)
                    {
                        if (d.Name == null)
                            return subdomains;
                        if (d.Name.Equals(""))
                            return subdomains;
                        Console.WriteLine(d.Name);
                        subdomains.Add(d);
                    }
                    i++;
                }
            }
            catch (Exception e)
            {

            }

            return subdomains;
        }

        public static void masGetAuthors(APIServiceClient client, uint domain,
            uint subdomain, uint i, List<Author> authors)
        {
            const uint HOWMANY = 100;

            try
            {
                Request request = new Request();
                request.AppID = AppID;
                request.ResultObjects = ObjectType.Author;
                request.DomainID = domain;
                request.SubDomainID = subdomain;
                request.OrderBy = OrderType.GIndex;
                request.StartIdx = i * HOWMANY + 1;
                request.EndIdx = i * HOWMANY + HOWMANY;
                Response response = client.Search(request);

                foreach (var a in response.Author.Result)
                {
                    if (a.HIndex > MAS_MINHINDEX && a.GIndex > MAS_MINGINDEX)
                        authors.Add(a);
                }

                /* Keep a low profile. They say under 200 request per min aprox. 333 ms */
                System.Threading.Thread.Sleep(MAS_DELAYMS);

            }
            catch (Exception e)
            {

            }
        }

        public static List<Author> masGetAuthorCitations(APIServiceClient client, uint domain,
            uint subdomain, uint authorid)
        {
            const uint HOWMANY = 100;
            List<Author> authorsciting = new List<Author>();
            try
            {
                Request request = new Request();
                request.AppID = AppID;
                request.ResultObjects = ObjectType.CitationContext;
                //request.DomainID = domain;
                //request.SubDomainID = subdomain;
                request.AuthorID = authorid;
                request.ReferenceType = ReferenceRelationship.Reference;
                for (uint i = 0; ; i++)
                {
                    request.StartIdx = i * HOWMANY + 1;
                    request.EndIdx = i * HOWMANY + HOWMANY;
                    Response response = client.Search(request);

                    if (response.Publication == null)
                        break;

                    if (response.Publication.Result == null)
                        break;

                    if (response.Publication.Result.Length < 1)
                        break;

                    foreach (var a in response.Publication.Result)
                    {
                        //authorsciting.Add(a);
                    }
                }
                /* Keep a low profile. They say under 200 request per min aprox. 333 ms */
                System.Threading.Thread.Sleep(MAS_DELAYMS);

            }
            catch (Exception e)
            {

            }

            return authorsciting;
        }

        public static void masGetOrganizations(APIServiceClient client, uint domain,
            uint subdomain, uint i, List<Organization> orgs)
        {
            
        }

        public static List<Publication> masGetPublications(APIServiceClient client, uint domain,
            uint subdomain, uint authorid)
        {
            const uint HOWMANY = 20;

            List<Publication> publications = new List<Publication>();
            try
            {
                Request request = new Request();
                request.AppID = AppID;
                request.ResultObjects = ObjectType.Publication;
                request.DomainID = domain;
                request.SubDomainID = subdomain;
                request.AuthorID = authorid;
                for (uint i = 0; ; i++ )
                {
                    request.StartIdx = i * HOWMANY + 1;
                    request.EndIdx = i * HOWMANY + HOWMANY;
                    Response response = client.Search(request);

                    if (response.Publication == null)
                        break;

                    if (response.Publication.Result == null)
                        break;

                    if (response.Publication.Result.Length < 1)
                        break;

                    foreach (var p in response.Publication.Result)
                    {
                        publications.Add(p);
                    }
                }
                /* Keep a low profile. They say under 200 request per min aprox. 333 ms */
                System.Threading.Thread.Sleep(MAS_DELAYMS);

            }
            catch (Exception e)
            {

            }

            return publications;
        }

        public static List<Publication> masGetPublicationCitations(APIServiceClient client, 
            uint pubid)
        {
            const uint HOWMANY = 100;
            List<Publication> publications = new List<Publication>();
            try
            {
                Request request = new Request();
                request.AppID = AppID;
                request.ResultObjects = ObjectType.Publication;
                request.ReferenceType = ReferenceRelationship.Citation;
                request.PublicationID = pubid;
                for (uint i = 0; ; i++)
                {
                    request.StartIdx = i * HOWMANY + 1;
                    request.EndIdx = i * HOWMANY + HOWMANY;
                    Response response = client.Search(request);

                    if (response.Publication == null)
                        break;

                    if (response.Publication.Result == null)
                        break;

                    if (response.Publication.Result.Length < 1)
                        break;

                    foreach (var p in response.Publication.Result)
                    {
                        publications.Add(p);
                    }
                }
                /* Keep a low profile. They say under 200 request per min aprox. 333 ms */
                System.Threading.Thread.Sleep(MAS_DELAYMS);

            }
            catch (Exception e)
            {

            }

            return publications;
        }
    }
}