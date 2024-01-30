import {
  Entity, ApiEntity, ComponentEntity, GroupEntity,
  ANNOTATION_ORIGIN_LOCATION, ANNOTATION_LOCATION, ANNOTATION_VIEW_URL,
} from '@backstage/catalog-model';
import {
  EntityProvider,
  EntityProviderConnection,
} from '@backstage/plugin-catalog-node';
import PQueue from 'p-queue';
import { parse } from 'csv-parse/sync';
import jsonSchemaGenerator from 'json-schema-generator';

type SchemaInfo = {
  type: string,
  definition: string,
  hasBom?: boolean,
  charset?: string,
};

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
    const entities = await this.applyDatasetEntities(datasetNames);

    console.log(`Entities:\t${entities.length}\n`);

    await this.connection.applyMutation({
      type: 'full',
      entities: [...entities].map(entity => ({
        entity,
        locationKey: `${this.getProviderName()}:${this.env}`,
      })),
    });
  }

  async applyDatasetEntities(datasetNames: string[]): Promise<Entity[]> {
    const datasets = await Promise.all(datasetNames.map(name => this.fetchDataset(name)));
    const datasetsPromises: Promise<Entity[]>[] = datasets
      .filter(dataset => {
        if (dataset.result.type !== "dataset") {
          console.warn(`Ignore type ${dataset.result.type} - could be a system? TODO: figure out how to find relationships to datasets.`);
          return false;
        } else {
          return true;
        }
      })
      .map(async (dataset, index) => {
        const group: GroupEntity = this.createOwner(dataset);
        const component: ComponentEntity = this.createComponent(dataset);
        const apis: ApiEntity[] = await this.createApis(dataset);
        if (index % 20 === 0) {
          console.log(`Progress: ${index} of ${datasets.length} (${Math.round(index / datasets.length * 100)}%)`);
        }
        return [
          group,
          component,
          ...apis,
        ]
      })
      .map(promise => promise.then(value => {
        return value;
      }))
      .map(async (datasetEntities) => {
        await this.connection!.applyMutation({
          type: 'delta',
          added: [...await datasetEntities].map(entity => ({
            entity,
            locationKey: `${this.getProviderName()}:${this.env}`,
          })),
          removed: [],
        });
        return datasetEntities;
      });
    const entitiesPromises: Entity[][] = await Promise.all(datasetsPromises);
    return entitiesPromises.flat();
  }

  async createApis(dataset: any): Promise<ApiEntity[]> {
    const data = dataset.result;
    const result = data.resources
      .map(async resource => {
        const schemaInfo: SchemaInfo = await this.tryDatastoreInfo(resource.id)
          .then(async datastoreSchema => {
            if (datastoreSchema !== undefined) {
              return datastoreSchema;
            } else {
              return await this.tryResourceUrl(resource.url);
            }
          })
          .catch(reason => {
            console.error(`Resource id failed: ${resource.id}`, reason);
            return { type: "error", definition: JSON.stringify(reason, null, 2) };
          }).then(schema => {
            if (schema === undefined) {
              console.warn(`Not found schema for ${resource.name} - ${resource.id} - ${resource.url}`)
              return { type: "unknown", definition: "unknown" };
            } else {
              return schema;
            }
          });

        const links = resource.url !== "" ? [{
          url: resource.url,
          title: "Link to the data file",
        }] : [];

        return {
          apiVersion: 'backstage.io/v1beta1',
          kind: 'API',
          metadata: {
            name: resource.id,
            title: resource.name === "" ? resource.url : resource.name,
            description: resource.description + (schemaInfo.hasBom ? "\n\n> :warning: **Datu fails satur BOM baitus!**" : ""),
            url: resource.url,
            format: resource.format,
            annotations: this.annotations(dataset.result.name),
            links: links,
          },
          spec: {
            type: schemaInfo.type,
            lifecycle: resource.state === "active" ? "production" : "experimental",
            owner: dataset.result.organization.name,
            definition: schemaInfo.definition,
            startsWithBom: schemaInfo.hasBom,
            charset: schemaInfo.charset,
            system: data.name,
          },
        } as ApiEntity;
      });
    return Promise.all(result);
  }

  async tryResourceUrl(url: string): Promise<SchemaInfo | undefined> {
    if (url.endsWith(".json")) {
      return { type: "solved", definition: "enable" };
      return this.tryResourceJsonUrl(url);
    } else if (url.endsWith(".csv")) {
      return { type: "solved", definition: "enable" };
      return this.tryResourceCsvUrl(url);
    } else if (url.endsWith(".xml")) {
      return { type: "xml", definition: "xml" }; // TODO: XSD?
      // } else if (url.endsWith(".docx")) { // All these following "datasets" :facepalm:
      //   return { type: "binary", definition: "docx" };
      // } else if (url.endsWith(".xlsm")) {
      //   return { type: "binary", definition: "xlsm" };
      // } else if (url.endsWith(".xlsx")) {
      //   return { type: "binary", definition: "xlsx" };
      // } else if (url.endsWith(".xls")) {
      //   return { type: "binary", definition: "xls" };
      // } else if (url.endsWith(".pdf")) {
      //   return { type: "binary", definition: "pdf" };
      // } else if (url.endsWith(".zip")) {
      //   return { type: "binary", definition: "zip" };
      // } else if (url.endsWith(".7z")) {
      //   return { type: "binary", definition: "7z" };
      // } else if (url.startsWith("https://public.tableau.com/")) {
      //   return { type: "tableau", definition: "tableau-dashboard" };
    } else if (url.endsWith(".geojson")) {
      return { type: "json-schema", definition: await fetch("https://geojson.org/schema/GeoJSON.json").then(response => response.text()) };
    }
    return undefined;
  }

  async tryResourceCsvUrl(url: string): Promise<SchemaInfo | undefined> {
    const response = await this.pqueue.add(() => fetch(url));
    if (response.status !== 200) {
      const responseText = await response.text();
      if (response.status !== 404) {
        console.error(`Resource id: ${url} returned ${response.status} - ${response.statusText}`);
        console.error(`${responseText}`);
      }
      return undefined;
    }

    const text = await response.text();

    let delimiter: string = ",";
    let fields: { name: string, type: string }[] = [];
    let foundOk = false;
    for (let tryOptions of [
      { delimiter: ";", relax_quotes: false },
      { delimiter: ",", relax_quotes: false },
      { delimiter: "|", relax_quotes: false },
      { delimiter: "\t", relax_quotes: false },
      // Stuff like this '=""; ="Nē"; =""; =""; =""; =""'...
      { delimiter: ";", relax_quotes: true },
      { delimiter: ",", relax_quotes: true },
      { delimiter: "|", relax_quotes: true },
      { delimiter: "\t", relax_quotes: true },
    ]) {
      try {
        const [headers, row]: string[][] = parse(text, {
          ...tryOptions,
          to: 2,
          cast: true,
          comment: "#",
          comment_no_infix: true,
          skip_empty_lines: true,
        });

        if (headers.length === 1) {
          delimiter = "unknown";
          fields = [{ name: headers[0], type: "string" }];
          continue;
        }

        delimiter = tryOptions.delimiter;
        fields = headers.map((header, index) => {
          return {
            name: header,
            type: row === undefined ? "string" :
              Number.isInteger(row[index]) ? "long"
                : typeof row[index] === 'number' ? "double"
                  : "string",
          };
        });
        foundOk = true;
        break;
      } catch {
        // try another delimiter
      }
    }

    if (!foundOk) {
      console.warn(`???? Fields for ${url}: ${JSON.stringify(fields)}`);
    }

    const avroSchema = {
      "type": "record",
      "name": "Row",
      "csv_delimiter": delimiter,
      "fields": fields,
    };

    return {
      type: "avro",
      definition: JSON.stringify(avroSchema, null, 2),
    };
  }

  async tryResourceJsonUrl(url: string): Promise<SchemaInfo | undefined> {
    const response = await this.pqueue.add(() => fetch(url));
    if (response.status !== 200) {
      const responseText = await response.text();
      if (response.status !== 404) {
        console.error(`Resource id: ${url} returned ${response.status} - ${response.statusText}`);
        console.error(`${responseText}`);
      }
      return undefined;
    }
    const data = new Uint8Array(await response.arrayBuffer());

    let hasBom = false;
    let charset = "utf8";
    let dataString: string;
    if (data[0] === 255 && data[1] === 254) { // BOM!
      hasBom = true;
      charset = "utf-16";
      dataString = new TextDecoder(charset).decode(data.slice(2));
    } else if (data[0] === 0xef && data[1] === 0xbb && data[2] === 0xbf) {
      hasBom = true;
      charset = "utf-8-sig";
      dataString = new TextDecoder().decode(data.slice(3));
    } else {
      dataString = new TextDecoder().decode(data);
    }

    const dataObject = JSON.parse(dataString);
    const definition = jsonSchemaGenerator(dataObject);
    return {
      type: "json-schema",
      definition: JSON.stringify(definition, null, 2),
      hasBom: hasBom,
      charset: charset,
    };
  }

  private static readonly TYPE_MAPPING = new Map<string, { avro: string }>([
    ["number", { avro: "double" }],
    ["text", { avro: "string" }],
    ["date", { avro: "string" }],
  ]);

  async tryDatastoreInfo(resourceId: string): Promise<SchemaInfo | undefined> {
    const response = await this.pqueue.add(
      () => fetch(DataGovLvProvider.API_INFO, {
        method: "POST",
        body: JSON.stringify({ id: resourceId }),
        headers: { 'Content-Type': 'application/json;charset=utf-8' },
      })
    );
    if (response.status !== 200) {
      const responseText = await response.text();
      if (response.status !== 404) {
        console.error(`Resource id: ${resourceId} returned ${response.status} - ${response.statusText}`);
        console.error(`${responseText}`);
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
          type: DataGovLvProvider.TYPE_MAPPING.get(value as string)?.avro ?? (function () {
            console.error(`TYPE: ${value} from ${resourceId}`);
            return "string";
          })(),
        };
      })
    }
    return {
      type: "avro",
      definition: JSON.stringify(avroSchema, null, 2),
    };
  }

  createComponent(dataset: any): ComponentEntity {
    const data = dataset.result;
    return {
      apiVersion: 'backstage.io/v1beta1',
      kind: 'Component',
      metadata: {
        name: data.name,
        title: data.title,
        description: data.notes,
        labels: {
          license_title: data.license_title,
          license_id: data.license_id,
          license_url: data.license_url,
          maintainer: data.maintainer,
          frequency: data.frequency,
        },
        tags: [...new Set<string>(data.tags.map((tag: { name: string; }) => tag.name.toLowerCase()))],
        annotations: {
          ...this.annotations(data.name),
          [ANNOTATION_VIEW_URL]: `url:https://data.gov.lv/dati/lv/dataset/${data.name}`,
        },
        links: [{
          url: `https://data.gov.lv/dati/lv/dataset/${data.name}`,
          title: "Link to data.gov.lv",
        }],
      },
      spec: {
        type: data.type,
        owner: data.organization.name,
        lifecycle: data.state === "active" ? "production" : "experimental",
        providesApis: data.resources.map(resource => resource.id),
      },
    };
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
