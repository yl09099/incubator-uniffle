import {createRouter, createWebHashHistory} from "vue-router"
import ApplicationPage from '@/components/ApplicationPage'
import CoordinatorServerPage from '@/components/CoordinatorServerPage'
import ShuffleServerPage from '@/components/ShuffleServerPage'
import ActiveNodeListPage from '@/components/shufflecomponent/ActiveNodeListPage'
import DecommissioningNodeListPage from '@/components/shufflecomponent/DecommissioningNodeListPage'
import DecommissionednodeListPage from '@/components/shufflecomponent/DecommissionednodeListPage'
import LostNodeList from '@/components/shufflecomponent/LostNodeList'
import UnhealthyNodeListPage from '@/components/shufflecomponent/UnhealthyNodeListPage'
import ExcludeNodeList from '@/components/shufflecomponent/ExcludeNodeList'

const routes = [
    {
        path: '/coordinatorserverpage',
        name: 'coordinatorserverpage',
        component: CoordinatorServerPage
    },
    {
        path: '/shuffleserverpage',
        name: 'shuffleserverpage',
        component: ShuffleServerPage,
        redirect: '/shuffleserverpage/activeNodeList',
        children: [
            {path: '/shuffleserverpage/activeNodeList', name: "activeNodeList", component: ActiveNodeListPage},
            {
                path: '/shuffleserverpage/decommissioningNodeList',
                name: "decommissioningNodeList",
                component: DecommissioningNodeListPage
            },
            {
                path: '/shuffleserverpage/decommissionedNodeList',
                name: "decommissionedNodeList",
                component: DecommissionednodeListPage
            },
            {path: '/shuffleserverpage/lostNodeList', name: "lostNodeList", component: LostNodeList},
            {path: '/shuffleserverpage/unhealthyNodeList', name: "unhealthyNodeList", component: UnhealthyNodeListPage},
            {path: '/shuffleserverpage/excludeNodeList', name: "excludeNodeList", component: ExcludeNodeList},
        ]
    },
    {
        path: '/applicationpage',
        name: 'applicationpage',
        component: ApplicationPage,
    },
]

const router = createRouter({
    history: createWebHashHistory(),
    routes
})

export default router