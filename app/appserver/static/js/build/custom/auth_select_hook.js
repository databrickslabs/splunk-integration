class AuthSelectHook {
    constructor(globalConfig, serviceName, state, mode, util) {
        this.globalConfig = globalConfig;
        this.serviceName = serviceName;
        this.state = state;
        this.mode = mode;
        this.util = util;
    }

    onChange(field, value, dataDict) {
        if (field == 'auth_type') {
            if (value == 'AAD') {
                this.toggleAADFields(true);
            } else {
                this.toggleAADFields(false);
            }
        }
        if (field == 'config_for_dbquery') {
            if (value == 'interactive_cluster') {
                this.hideWarehouseField(false);
            } else {
                this.hideWarehouseField(true);
            }
        }
    }

    onRender() {
        var selected_auth = this.state.data.auth_type.value;
        if (selected_auth == 'AAD') {
            this.toggleAADFields(true);
        } else {
            this.toggleAADFields(false);
        }
    }

    hideWarehouseField(state) {
        this.util.setState((prevState) => {
            let data = {...prevState.data };
            data.warehouse_id.display = state;
            return { data }
        }); 
    }

    toggleAADFields(state) {
        this.util.setState((prevState) => {
            let data = {...prevState.data };
            data.aad_client_id.display = state;
            data.aad_tenant_id.display = state;
            data.aad_client_secret.display = state;
            data.databricks_pat.display = !state;
            return { data }
        });
    }

}

export default AuthSelectHook;