import {
  Entity, SystemEntity, ApiEntity, ComponentEntity, GroupEntity,
  ANNOTATION_ORIGIN_LOCATION, ANNOTATION_LOCATION, ANNOTATION_VIEW_URL,
} from '@backstage/catalog-model';
import {
  EntityProvider,
  EntityProviderConnection,
} from '@backstage/plugin-catalog-node';
import PQueue from 'p-queue';


export class DataGovLvProvider implements EntityProvider {
  private static readonly API_LIST = "https://data.gov.lv/dati/lv/api/3/action/package_list";
  private static readonly API_SHOW = "https://data.gov.lv/dati/lv/api/3/action/package_show";
  private static readonly API_INFO = "https://data.gov.lv/dati/lv/api/3/action/datastore_info";

  private readonly env: string;
  private connection?: EntityProviderConnection;

  private readonly pqueue = new PQueue({ concurrency: 10, });

  constructor(env: string) {
    this.env = env;
  }

  getProviderName(): string {
    return `datagovlv-${this.env}`;
  }

  async connect(connection: EntityProviderConnection): Promise<void> {
    this.connection = connection;
  }

  async run(): Promise<void> {
    if (!this.connection) {
      throw new Error('Not initialized');
    }

    const datasetNames = await this.getDatasetNames();
    const entities = await this.datasetEntities(datasetNames);

    console.log(`Entities:\t${entities.length}\n`);

    await this.connection.applyMutation({
      type: 'full',
      entities: [...entities].map(entity => ({
        entity,
        locationKey: `${this.getProviderName()}:${this.env}`,
      })),
    });
  }

  async datasetEntities(datasetNames: string[]): Promise<Entity[]> {
    const datasets = await Promise.all(datasetNames.map(name => this.fetchDataset(name)));
    const datasetsPromises: Promise<Entity[]>[] = datasets.map(async (dataset, index) => {
      const system: SystemEntity = this.createSystem(dataset);
      const group: GroupEntity = this.createOwner(dataset);
      const components: ComponentEntity[] = this.createComponents(dataset);
      const apis: ApiEntity[] = await this.createApis(dataset);
      if (index % 20 === 0) {
        console.log(`Progress: ${index} of ${datasets.length} (${Math.round(index / datasets.length * 100)}%)`);
      }
      return [
        system,
        group,
        ...components,
        ...apis,
      ]
    }).map(promise => promise.then(value => {
      return value;
    }));
    const entitiesPromises: Entity[][] = await Promise.all(datasetsPromises);
    return entitiesPromises.flat();
  }

  async createApis(dataset: any): Promise<ApiEntity[]> {
    const data = dataset.result;
    const result = data.resources
      .map(async resource => {
        let schemaInfo = await this.tryDatastoreInfo(resource.id)
          .catch(reason => {
            console.error(`Resource id failed: ${resource.id}`, reason);
            console.log(reason);
            return { type: "unknown", definition: "unknown" };
          });
        if (schemaInfo === undefined) {
          schemaInfo = { type: "unknown", definition: "unknown" };
        }
        return {
          apiVersion: 'backstage.io/v1beta1',
          kind: 'API',
          metadata: {
            name: resource.id,
            title: resource.name === "" ? resource.url : resource.name,
            description: resource.description,
            url: resource.url,
            format: resource.format,
            annotations: this.annotations(dataset.result.name),
          },
          spec: {
            type: schemaInfo.type,
            lifecycle: resource.state === "active" ? "production" : "experimental",
            owner: dataset.result.organization.name,
            definition: schemaInfo.definition,
            system: data.name,
          },
        } as ApiEntity;
      });
    return (await Promise.all(result)).flat();
  }

  private static readonly TYPE_MAPPING = new Map<string, { avro: string }>([
    ["number", { avro: "double" }],
    ["text", { avro: "string" }],
    ["date", { avro: "string" }],
  ]);

  async tryDatastoreInfo(resourceId: string): Promise<{ type: string, definition: string } | undefined> {
    const response = await this.pqueue.add(
      () => fetch(DataGovLvProvider.API_INFO, {
        method: "POST",
        body: JSON.stringify({ id: resourceId }),
        headers: { 'Content-Type': 'application/json;charset=utf-8' },
      })
    );
    if (response.status !== 200) {
      if (response.status !== 404) {
        console.error(`Resource id: ${resourceId} returned ${response.status} - ${response.statusText}`);
        console.error(`${await response.text()}`);
      }
      return undefined;
    }
    const responseData = await response.json();
    if (responseData.success === false) {
      console.error(`Resource id: ${resourceId} returned ${responseData}`);
      return undefined;
    }
    const schema = responseData.result.schema;
    const avroSchema = {
      "type": "record",
      "name": "Row",
      "namspace": resourceId,
      "fields": Object.entries(schema).map(([key, value]) => {
        return {
          name: key,
          type: DataGovLvProvider.TYPE_MAPPING.get(value as string)?.avro ?? (function () { console.error(`TIPS: ${value} no ${resourceId}`); return "string"; })()
        };
      })
    }
    return {
      type: "avro",
      definition: JSON.stringify(avroSchema, null, 2),
    };
  }

  createComponents(dataset: any): ComponentEntity[] {
    const data = dataset.result;
    return data.resources
      .map(resource => {
        return {
          apiVersion: 'backstage.io/v1beta1',
          kind: 'Component',
          metadata: {
            name: resource.id,
            title: resource.name === "" ? resource.url : resource.name,
            description: resource.description,
            labels: {
              format: resource.format,
              frequency: data.frequency,
              url: resource.url,
              license_title: data.license_title,
              license_id: data.license_id,
              license_url: data.license_url,
              maintainer: data.maintainer,
            },
            tags: [...new Set<string>(data.tags.map((tag: { name: string; }) => tag.name.toLowerCase()))],
            annotations: {
              ...this.annotations(dataset.result.name),
              [ANNOTATION_VIEW_URL]: `url:https://data.gov.lv/dati/lv/dataset/${dataset.result.name}/resource/${resource.id}`,
            },
          },
          spec: {
            type: data.type,
            lifecycle: resource.state === "active" ? "production" : "experimental",
            owner: dataset.result.organization.name,
            system: data.name,
            providesApis: [resource.id]
          },
        } as ComponentEntity
      });
  }

  createOwner(dataset: any): GroupEntity {
    const org = dataset.result.organization;
    return {
      apiVersion: 'backstage.io/v1beta1',
      kind: 'Group',
      metadata: {
        name: org.name,
        title: org.title,
        description: org.description,
        annotations: this.annotations(dataset.result.name),
      },
      spec: {
        type: org.is_organization ? "organization" : "non-organization",
        profile: {
          email: dataset.result.maintainer_email,
          picture: `https://data.gov.lv/dati/uploads/group/${org.image_url}`,
        },
        children: [],
      },
    }
  }

  createSystem(dataset: any): SystemEntity {
    const data = dataset.result;
    return {
      apiVersion: 'backstage.io/v1beta1',
      kind: 'System',
      metadata: {
        name: data.name,
        title: data.title,
        description: data.notes,
        labels: {
          license_title: data.license_title,
          license_id: data.license_id,
          license_url: data.license_url,
          maintainer: data.maintainer,
        },
        tags: [...new Set<string>(data.tags.map((tag: { name: string; }) => tag.name.toLowerCase()))],
        annotations: {
          ...this.annotations(data.name),
          [ANNOTATION_VIEW_URL]: `url:https://data.gov.lv/dati/lv/dataset/${data.name}`,
        },
      },
      spec: {
        owner: dataset.result.organization.name,
      },
    };
  }

  async fetchDataset(name: string): Promise<any> {
    return await this.pqueue.add(
      () => fetch(DataGovLvProvider.API_SHOW + "?id=" + name)
        .then(response => response.json())
    );
  }

  async getDatasetNames(): Promise<string[]> {
    const response = await fetch(DataGovLvProvider.API_LIST);
    const data = await response.json();
    return data.result;
  }

  annotations(id: string) {
    return {
      [ANNOTATION_LOCATION]: `url:${DataGovLvProvider.API_SHOW}?id=${id}`,
      [ANNOTATION_ORIGIN_LOCATION]: `url:${DataGovLvProvider.API_SHOW}?id=${id}`,
    }
  }
}
