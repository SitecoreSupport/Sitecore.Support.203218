using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Xml;
using Sitecore.Analytics.DataAccess;
using Sitecore.Analytics.Model.Entities;
using Sitecore.Analytics.Processing;
using Sitecore.Analytics.XConnect.Facets;
using Sitecore.Data;
using Sitecore.Framework.Conditions;
using Sitecore.XConnect;
using Sitecore.XConnect.Client;
using Sitecore.XConnect.Collection.Model;
using Sitecore.Xml;
using XdbUnavailableException = Sitecore.Analytics.DataAccess.XdbUnavailableException;

namespace Sitecore.Support.Analytics.XConnect.DataAccess
{
  public class XConnectDataAdapterProvider : Sitecore.Analytics.XConnect.DataAccess.XConnectDataAdapterProvider
  {
    internal IXdbContextFactory ContextFactory
    {
      get
      {
        return typeof(Sitecore.Analytics.XConnect.DataAccess.XConnectDataAdapterProvider).GetField("ContextFactory",
          BindingFlags.GetField | BindingFlags.Instance | BindingFlags.NonPublic).GetValue(this) as IXdbContextFactory;
      }
    }

    private string[] _facetsToLoad = Array.Empty<string>();
    protected new void AddFacet(XmlNode configNode)
    {
      Condition.Requires<string>(configNode.Name, "configNode.Name").IsEqualTo("facet");
      string attribute = XmlUtil.GetAttribute("facetKey", configNode);
      this.AddFacet(attribute);
      base.AddFacet(configNode);
    }
    internal new void AddFacet(string facetKey)
    {
      HashSet<string> hashSet = new HashSet<string>(this._facetsToLoad);
      if (hashSet.Add(facetKey))
      {
        string[] value = hashSet.ToArray<string>();
        Interlocked.Exchange<string[]>(ref this._facetsToLoad, value);
      }
    }
    private Sitecore.XConnect.Contact GetContactByTrackerContactId(ID contactId)
    {
      Condition.Requires<ID>(contactId, "contactId").IsNotNull<ID>();
      return this.ExecuteWithExceptionHandling<Sitecore.XConnect.Contact>((IXdbContext xdbContext) => this.GetContactByTrackerContactId(xdbContext, contactId));
    }
    private Sitecore.XConnect.Contact GetContactByTrackerContactId(IXdbContext xdbContext, ID contactId)
    {
      Condition.Requires<IXdbContext>(xdbContext, "xdbContext").IsNotNull<IXdbContext>();
      Condition.Requires<ID>(contactId, "contactId").IsNotNull<ID>();
      return this.GetContactByIdentifier(xdbContext, "xDB.Tracker", ToXConnectIdentifier(contactId.Guid));
    }
    private Sitecore.XConnect.Contact GetContactByIdentifier(IXdbContext xdbContext, string source, string identifier)
    {
      Condition.Requires<IXdbContext>(xdbContext, "xdbContext").IsNotNull<IXdbContext>();
      Condition.Requires<string>(source, "source").IsNotNull<string>();
      Condition.Requires<string>(identifier, "identifier").IsNotNull<string>();
      ExpandOptions expandOptions = new ExpandOptions(this._facetsToLoad);
      Sitecore.XConnect.Contact contact = xdbContext.Get(new IdentifiedContactReference(source, identifier), expandOptions, this.GetOperationTimeout);
      MergeInfo mergeInfo = (contact != null) ? contact.MergeInfo() : null;
      Guid? guid = (mergeInfo != null && mergeInfo.Obsolete) ? new Guid?(mergeInfo.SuccessorContactId) : null;
      if (guid.HasValue)
      {
        return xdbContext.Get(new ContactReference(guid.Value), expandOptions, this.GetOperationTimeout);
      }

      return contact;
    }

    public override bool SaveContact(IContact contact, ContactSaveOptions contactSaveOptions)
    {
      Condition.Requires(contact, nameof(contact)).IsNotNull();
      Condition.Requires(contactSaveOptions, nameof(contactSaveOptions)).IsNotNull();

      return
          ExecuteWithExceptionHandling(xdbContext =>
          {
            bool? contactIsNew = contactSaveOptions.IsNew;
            if (!contactIsNew.HasValue)
            {
              contactIsNew = GetContactByTrackerContactId(contact.Id) == null;
            }

            if (contactIsNew.Value)
            {
              var trackingIdentifier = new Sitecore.XConnect.ContactIdentifier(
                "xDB.Tracker", ToXConnectIdentifier(contact.Id.Guid), ContactIdentifierType.Anonymous);

              var xConnectContact = new Sitecore.XConnect.Contact(trackingIdentifier);
              xdbContext.AddContact(xConnectContact);

              Classification classification = new Classification();
              CopySystemData(contact.System, classification);

              xdbContext.SetClassification(xConnectContact, classification);

              xdbContext.Submit();
            }
            else
            {
              Classification classification = GetXConnectClassificationFacet(contact);
              if (classification != null &&
                        classification.ClassificationLevel == contact.System.Classification &&
                        classification.OverrideClassificationLevel == contact.System.OverrideClassification)
              {
                return true;
              }

              //patch
              if (classification == null)
              {
                classification = GetClassificationFromXConnect(xdbContext, contact);
              }
              //patch end

              classification = classification ?? new Classification();

              CopySystemData(contact.System, classification);

              xdbContext.SetClassification(
                        new IdentifiedContactReference("xDB.Tracker", ToXConnectIdentifier(contact.Id.Guid)),
                        classification);

              xdbContext.Submit();
            }

            return true;
          });
    }
    private T ExecuteWithExceptionHandling<T>(
      Func<IXdbContext, T> func)
    {
      try
      {
        using (IXdbContext xdbContext = ContextFactory.Create())
        {
          return func(xdbContext);
        }
      }

      catch (XdbUnavailableException e)
      {
        throw new Sitecore.Analytics.DataAccess.XdbUnavailableException(e);
      }
    }

    public static void CopySystemData(IContactSystemInfo systemInfoEntity, Classification classification)
    {
      Condition.Requires<IContactSystemInfo>(systemInfoEntity, "systemInfoEntity").IsNotNull<IContactSystemInfo>();
      Condition.Requires<Classification>(classification, "classification").IsNotNull<Classification>();
      classification.ClassificationLevel = systemInfoEntity.Classification;
      classification.OverrideClassificationLevel = systemInfoEntity.OverrideClassification;
    }

    public static string ToXConnectIdentifier(Guid contactId)
    {
      Condition.Requires<Guid>(contactId, "contactId").IsNotNull<Guid>();
      return contactId.ToString("N");
    }

    private Classification GetXConnectClassificationFacet(IContact contact)
    {
      if (!contact.Facets.ContainsKey("XConnectFacets"))
      {
        return null;
      }

      IXConnectFacets facet = contact.GetFacet<IXConnectFacets>("XConnectFacets");
      if (((facet != null) ? facet.Facets : null) == null)
      {
        return null;
      }

      if (!facet.Facets.ContainsKey("Classification"))
      {
        return null;
      }

      return facet.Facets["Classification"] as Classification;
    }

    private Classification GetClassificationFromXConnect(IXdbContext context, IContact contact)
    {
      var references = new List<IEntityReference<Sitecore.XConnect.Contact>>()
      {
        // Contact ID
        new ContactReference(contact.Id.Guid)
      };

      var retrievedContact = GetContactByTrackerContactId(contact.Id);

      if (retrievedContact != null)
      {
        return retrievedContact.GetFacet<Classification>("Classification");
      }

      return null;
    }
  }
}