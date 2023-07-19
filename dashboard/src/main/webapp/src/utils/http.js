import request from "@/utils/request";
const http = {
    get(url, params, headers) {
        const config = {
            method: 'GET',
            url: url,
            params: params,
            headers: headers
        }
        return request(config);
    },
    post(url, data, headers) {
        const config = {
            method: 'POST',
            url: url,
            data: data,
            headers: headers
        }
        return request(config);
    }
}
export default http