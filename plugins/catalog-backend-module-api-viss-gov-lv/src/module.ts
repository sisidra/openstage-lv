import { coreServices, createBackendModule } from '@backstage/backend-plugin-api';

export const catalogModuleApiVissGovLv = createBackendModule({
  pluginId: 'catalog',
  moduleId: 'api-viss-gov-lv',
  register(reg) {
    reg.registerInit({
      deps: { logger: coreServices.logger },
      async init({ logger }) {
        logger.info('Hello World!')
      },
    });
  },
});
