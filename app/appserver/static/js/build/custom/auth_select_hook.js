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
    }

    onRender() {
        var selected_auth = this.state.data.auth_type.value;
        if (selected_auth == 'AAD') {
            this.toggleAADFields(true);
        } else {
            this.toggleAADFields(false);
        }

    }

    toggleAADFields(state) {
        this.util.setState((prevState) => {
            let data = {...prevState.data };
            data.client_id.display = state;
            data.tenant_id.display = state;
            data.client_secret.display = state;
            data.databricks_access_token.display = !state;
            return { data }
        });
    }

}

export default AuthSelectHook;