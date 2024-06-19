import { CatalogBuilder } from '@backstage/plugin-catalog-backend';
import { ScaffolderEntitiesProcessor } from '@backstage/plugin-catalog-backend-module-scaffolder-entity-model';
import { Router } from 'express';
import { PluginEnvironment } from '../types';
import { DataGovLvProvider } from '../../../../plugins/catalog-backend-module-data-gov-lv/src/datagovlv-provider';
import { ApiVissGovLvProvider } from '../../../../plugins/catalog-backend-module-api-viss-gov-lv/src/apivissgovlv-provider';

export default async function createPlugin(
  env: PluginEnvironment,
): Promise<Router> {
  const builder = CatalogBuilder.create(env);

  builder.setFieldFormatValidators({
    // Default is KubernetesValidatorFunctions.isValidObjectName, which does not allow >63 char names
    isValidEntityName: () => true,
    // Allow all tags and labels
    isValidTag: () => true,
    isValidLabelValue: () => true,
  });

  builder.addProcessor(new ScaffolderEntitiesProcessor());

  const dataGovLv = new DataGovLvProvider('production');
  const apiVissGovLv = new ApiVissGovLvProvider('production');
  builder.addEntityProvider(dataGovLv);
  builder.addEntityProvider(apiVissGovLv);

  const { processingEngine, router } = await builder.build();
  await processingEngine.start();

  await env.scheduler.scheduleTask({
    id: 'run_dataGovLv_refresh',
    fn: async () => {
      await dataGovLv.run();
    },
    frequency: { minutes: 300 },
    timeout: { minutes: 200 },
  });
  await env.scheduler.scheduleTask({
    id: 'run_apiVissGovLv_refresh',
    fn: async () => {
      await apiVissGovLv.run();
    },
    frequency: { minutes: 300 },
    timeout: { minutes: 200 },
  });

  return router;
}
