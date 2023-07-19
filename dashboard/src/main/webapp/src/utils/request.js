import axios from 'axios'

const axiosInstance = axios.create({
    baseURL: 'http://localhost:8080/api',
    timeout: 10000,
    headers: {}
})

axiosInstance.interceptors.request.use(config => {
    config.headers['Content-type'] = 'application/json';
    config.headers['Accept'] = 'application/json';
    return config;
})

axiosInstance.interceptors.response.use(response => {
    return response;
}, error => {
    return error;
})
export default axiosInstance;
