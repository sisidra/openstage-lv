import {
  ApiEntity, ComponentEntity, GroupEntity,
  ANNOTATION_ORIGIN_LOCATION, ANNOTATION_LOCATION, ANNOTATION_VIEW_URL,
} from '@backstage/catalog-model';
import {
  EntityProvider,
  EntityProviderConnection,
} from '@backstage/plugin-catalog-node';
import PQueue from 'p-queue';

type ApiListItem = {
  id: string
  name: string
  description: string | null
  context: string
  version: string
  type: "HTTP" | "SOAP"
  provider: string
  lifeCycleStatus: "PUBLISHED" | "CREATED"
  thumbnailUri: string | null
  avgRating: string  // "0.0" | "5.0" | "3.5" | "4.0"
  throttlingPolicies: ("Unlimited" | "Bronze" | "Gold" | "Silver")[]
  isSubscriptionAvailable: boolean
  [k: string]: unknown
};

type ApiDefinition = {
  id: string
  name: string
  description: string | null
  context: string
  version: string
  provider: string
  apiDefinition: string
  wsdlUri: null
  lifeCycleStatus: "PUBLISHED"
  isDefaultVersion: boolean
  type: "HTTP"
  transport: ("http" | "https")[]
  authorizationHeader: null | string
  securityScheme: (
    | "oauth2"
    | "oauth_basic_auth_api_key_mandatory"
    | "mutualssl"
  )[]
  tags: string[]
  tiers: {
    tierName: "Unlimited" | "Bronze" | "Gold" | "Silver"
    tierPlan: "FREE"
    monetizationAttributes: null
  }[]
  additionalProperties: {
    name: "transactionRequired"
    value: "false" | "true"
    display: boolean
  }[]
  endpointURLs: {
    environmentName: string
    environmentDisplayName: string
    environmentType: string
    URLs: {
      http: string | null
      https: string
      ws: null
      wss: null
    }
    defaultVersionURLs: {
      http: string | null
      https: string | null
      ws: null
      wss: null
    }
  }[]
  businessInformation: {
    businessOwner: string | null
    businessOwnerEmail: null | string
    technicalOwner: null | string
    technicalOwnerEmail: null | string
  }
  scopes: {
    key: string
    name: string
    description: string
  }[]
  avgRating: string
  advertiseInfo: {
    apiOwner: null | string
  }
  isSubscriptionAvailable: boolean
  categories: string[]
  [k: string]: unknown
}

export class ApiVissGovLvProvider implements EntityProvider {
  private static readonly API_LIST = "https://api.viss.gov.lv/api/am/devportal/v2/apis?limit=1000&offset=0";
  private static readonly API_SHOW = "https://api.viss.gov.lv/api/am/devportal/v2/apis/{ID}";
  private static readonly API_DOCS = "https://api.viss.gov.lv/api/am/devportal/v2/apis/{ID}/documents";
  private static readonly API_WSDL = "https://api.viss.gov.lv";

  private readonly env: string;
  private connection?: EntityProviderConnection;

  private readonly pqueue = new PQueue({ concurrency: 10, });

  constructor(env: string) {
    this.env = env;
  }

  getProviderName(): string {
    return `apivissgovlv-${this.env}`;
  }

  async connect(connection: EntityProviderConnection): Promise<void> {
    this.connection = connection;
  }

  async run(): Promise<void> {
    if (!this.connection) {
      throw new Error('Not initialized');
    }

    const apis = await this.listApis();
    const apiDefinitions = this.promiseApiDefinitions(apis);

    const components = this.transformToComponents(apiDefinitions);
    const apiComponents = this.transformToApis(apiDefinitions);
    const groups = this.transformToGroups(apiDefinitions);

    console.log(`Components:\t${components.length}`);
    console.log(`APIs:\t${apiComponents.length}`);
    console.log(`Groups:\t${groups.length}`);

    await this.connection.applyMutation({
      type: 'full',
      entities: [
        ...await Promise.all(components),
        ...await Promise.all(apiComponents),
        ...await Promise.all(groups),
      ].map(entity => ({
        entity,
        locationKey: `${this.getProviderName()}:${this.env}`,
      })),
    });
  }

  transformToGroups(apis: Promise<ApiDefinition>[]): Promise<GroupEntity>[] {
    return apis.map(api => this.transformToGroup(api));
  }
  async transformToGroup(apiPromise: Promise<ApiDefinition>): Promise<GroupEntity> {
    const api = await apiPromise;

    const name = api.businessInformation.businessOwner ?? api.businessInformation.technicalOwner ?? api.provider;

    return {
      apiVersion: 'backstage.io/v1beta1',
      kind: 'Group',
      metadata: {
        name: name,
        annotations: this.annotations(api.id),
      },
      spec: {
        type: "bureaucrat",
        profile: {
          email: api.businessInformation.businessOwnerEmail ?? api.businessInformation.technicalOwnerEmail ?? undefined,
          picture: `https://api.viss.gov.lv/api/am/devportal/v2/apis/${api.id}/thumbnail`,
        },
        children: [],
      },
    }
  }

  transformToApis(apis: Promise<ApiDefinition>[]): Promise<ApiEntity>[] {
    return apis.map(api => this.transformToApi(api));
  }

  async transformToApi(apiPromise: Promise<ApiDefinition>): Promise<ApiEntity> {
    const api = await apiPromise;
    return {
      apiVersion: 'backstage.io/v1beta1',
      kind: 'API',
      metadata: {
        name: api.name,
        description: (api.description ?? "") +
          (api.wsdlUri ? `\n\n!!! WSDL: ${ApiVissGovLvProvider.API_WSDL + api.wsdlUri}` : ""),
        annotations: this.annotations(api.id),
        links: [{
          url: `https://api.viss.gov.lv/devportal/apis/${api.id}/overview`,
          title: "Backlink to api.viss.gov.lv",
        }],
      },
      spec: {
        type: api.wsdlUri ? "WSDL" : "swagger",
        lifecycle: api.lifeCycleStatus === "PUBLISHED" ? "production" : "experimental",
        owner: api.businessInformation.businessOwner ?? api.businessInformation.technicalOwner ?? api.provider,
        // WSDL behind auth
        // definition: api.wsdlUri
        //   ? { $text: ApiVissGovLvProvider.API_WSDL + api.wsdlUri }
        //   : api.apiDefinition,
        definition: api.apiDefinition,
        // system: data.name,
      },
    };
  }

  transformToComponents(apis: Promise<ApiDefinition>[]): Promise<ComponentEntity>[] {
    return apis.map(api => this.transformToComponent(api));
  }

  async transformToComponent(apiPromise: Promise<ApiDefinition>): Promise<ComponentEntity> {
    const api = await apiPromise;
    return {
      apiVersion: 'backstage.io/v1beta1',
      kind: 'Component',
      metadata: {
        name: api.name,
        description: api.description ?? undefined,
        labels: {
        },
        tags: api.tags,
        annotations: {
          version: api.version,
          provider: api.provider,
          avgRating: api.avgRating,
          ...this.annotations(api.id),
          [ANNOTATION_VIEW_URL]: `url:https://api.viss.gov.lv/devportal/apis/${api.id}/overview`,
        },
        links: [{
          url: `https://api.viss.gov.lv/devportal/apis/${api.id}/overview`,
          title: "Backlink to api.viss.gov.lv",
        }],
      },
      spec: {
        type: api.type,
        owner: api.businessInformation.businessOwner ?? api.businessInformation.technicalOwner ?? api.provider,
        lifecycle: api.lifeCycleStatus === "PUBLISHED" ? "production" : "experimental",
        providesApis: [api.name],
      },
    };
  }

  promiseApiDefinitions(apis: ApiListItem[]): Promise<ApiDefinition>[] {
    return apis.map(api => this.getApiDefinition(api));
  }

  async getApiDefinition(api: ApiListItem): Promise<ApiDefinition> {
    async function getJson(): Promise<ApiDefinition> {
      const url = ApiVissGovLvProvider.API_SHOW.replace("{ID}", api.id);
      const response = await fetch(url);
      return response.json();
    }

    return await this.pqueue.add(getJson);
  }

  async listApis(): Promise<ApiListItem[]> {
    const response = await fetch(ApiVissGovLvProvider.API_LIST);
    const data = await response.json();
    return data.list as ApiListItem[];
  }

  annotations(id: string) {
    const url = ApiVissGovLvProvider.API_SHOW.replace("{ID}", id);
    return {
      [ANNOTATION_LOCATION]: `url:${url}`,
      [ANNOTATION_ORIGIN_LOCATION]: `url:${url}`,
    }
  }
}
