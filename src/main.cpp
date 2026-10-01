#include"Node.h"
#include<iostream>
#include<cstdlib>

int main(int argc,char* argv[])
{
    try
    {
        if(argc!=3)
        {
            std::cerr<<"Usage: "<<argv[0]<<" <node_id><listen_port>";
            return 1;
        }

        NodeId node_id=static_cast<NodeId>(std::stoul(argv[1]));
        uint16_t port=static_cast<uint16_t>(std::stoi(argv[2]));

        Node node(node_id,port);
        node.run();
    }
    catch(const std::system_error& e)
    {
        std::cerr <<"Error:"<< e.what() <<"(code"<<e.code()<<")"<<std::endl;
        return 1;
    }
    catch(const std::exception& e)
    {
        std::cerr <<"Exception:"<< e.what()<<std::endl;
        return 1;
    }
    return 0;
    
}